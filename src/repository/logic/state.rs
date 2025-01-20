use tokio::fs;
use log::{debug, error};
use tokio::task::JoinHandle;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::mpsc::error::SendError;
use thiserror::Error;
use std::os::unix::fs::MetadataExt;
use crate::db::models::{FileEqBlobCheck, FileSeen, Observation, VirtualFile, VirtualFileState};
use crate::repository::traits::{Local, VirtualFilesystem};
use crate::utils::control_flow::Message;
use crate::utils::walker;
use crate::utils::walker::{walk, FileObservation, WalkerConfig};

pub struct StateConfig {
    buffer_size: usize,
    walker: WalkerConfig,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1000,
            walker: WalkerConfig::default(),
        }
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("sqlx error: {0}")]
    SQLXError(#[from] sqlx::Error),
    #[error("fs walker error: {0}")]
    WalkerError(#[from] walker::Error),
    #[error("observation send error: {0}")]
    SendError(String),
    #[error("task execution failed: {0}")]
    TaskFailure(String),
}

impl<T> From<SendError<T>> for Error {
    fn from(value: SendError<T>) -> Self {
        Error::SendError(value.to_string())
    }
}

fn current_timestamp() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_secs() as i64
}

async fn are_hardlinked(path1: &std::path::Path, path2: &std::path::Path) -> std::io::Result<bool> {
    let metadata1 = fs::metadata(path1).await?;
    let metadata2 = fs::metadata(path2).await?;

    let result = metadata1.dev() == metadata2.dev() && metadata1.ino() == metadata2.ino();
    debug!(
        "are_hardlinked: path1: {:?}, path2: {:?} result {:}",
        path1, path2, result,
    );
    Ok(result)
}

async fn check(vfs: &impl Local, vf: VirtualFile) -> Result<Observation, Error> {
    let last_result = match vf.blob_id {
        None => false,
        Some(blob_id) => {
            are_hardlinked(&vfs.root().join(vf.path.clone()), &vfs.blob_path(blob_id)).await?
        }
    };

    Ok(Observation::FileEqBlobCheck(FileEqBlobCheck {
        path: vf.path.clone(),
        last_check_dttm: current_timestamp(),
        last_result,
    }))
}

async fn close(
    tx: Sender<Result<VirtualFile, Error>>,
    vfs: impl VirtualFilesystem + Local + Send + Sync + Clone,
    last_seen_id: i64,
) -> Result<(), Error> {
    debug!("close: closing virtual filesystem");

    vfs.cleanup(last_seen_id).await?;

    let mut deleted_files = vfs.select_deleted_files(last_seen_id).await;
    while let Some(r) = deleted_files.next().await {
        match r {
            Ok(vf) => {
                if let Err(e) = tx.send(Ok(vf)).await {
                    panic!("failed to send element to output stream: {}", e);
                }
            }
            Err(e) => {
                if let Err(e_send) = tx.send(Err(e.into())).await {
                    panic!("failed to send error to output stream: {}", e_send);
                }
            }
        }
    }

    Ok(())
}

pub async fn state(
    vfs: impl VirtualFilesystem + Local + Send + Sync + Clone + 'static,
    config: StateConfig,
) -> Result<
    (
        JoinHandle<Result<(), Error>>,
        impl Stream<Item = Result<VirtualFile, Error>>,
    ),
    Box<dyn std::error::Error>,
> {
    let root = vfs.root();
    let last_seen_id = current_timestamp();

    vfs.refresh().await?;
    debug!("virtual filesystem refreshed");

    /* Three channels:
       - obs: observations by the filesystem walker
       - needs_check: files which need to be looked at
       - output channel
    */
    let (obs_tx, obs_rx) = mpsc::channel::<Message<Observation>>(config.buffer_size);
    let (needs_check_tx, mut needs_check_rx): (
        Sender<Message<VirtualFile>>,
        Receiver<Message<VirtualFile>>,
    ) = mpsc::channel(config.buffer_size);
    let (tx, rx): (
        Sender<Result<VirtualFile, Error>>,
        Receiver<Result<VirtualFile, Error>>,
    ) = mpsc::channel(config.buffer_size);

    // get the channel of the filesystem walker
    let (walker_handle, mut walker_rx) = walk(root, config.walker).await?;

    // connects the observation channel with the DB and receives the results via the db output stream
    let db_output_stream = vfs.add_observations(ReceiverStream::new(obs_rx)).await;

    // transforms the files seen by the filesystem walker into observations suitable for the DB
    let tx_clone = tx.clone();
    let obs_tx_clone = obs_tx.clone();
    let walker_transformer_handle: JoinHandle<()> = tokio::spawn(async move {
        while let Some(file_result) = walker_rx.recv().await {
            match file_result {
                Ok(FileObservation {
                    rel_path,
                    size,
                    last_modified,
                }) => {
                    let observation = Observation::FileSeen(FileSeen {
                        path: rel_path.to_string_lossy().to_string(),
                        last_seen_id,
                        last_seen_dttm: current_timestamp(),
                        last_modified_dttm: last_modified,
                        size,
                    });
                    if let Err(e) = obs_tx_clone.send(observation.into()).await {
                        if let Err(e_send) = tx_clone.send(Err(e.into())).await {
                            panic!("failed to send error to output stream: {}", e_send);
                        }
                    }
                }
                Err(e) => {
                    if let Err(e_send) = tx_clone.send(Err(e.into())).await {
                        panic!("failed to send error to output stream: {}", e_send);
                    }
                }
            }
        }

        // close the channels once the stream is exhausted.
        if let Err(e) = obs_tx_clone.send(Message::Shutdown).await {
            panic!("failed to send error to output stream: {}", e);
        }
    });

    // splits the observations by the filesystem walker into:
    // - output channel: for states new/dirty/ok
    // - needs_check channel: needs_check or unknown
    let tx_clone = tx.clone();
    let needs_check_tx_clone = needs_check_tx.clone();
    let splitter_handle: JoinHandle<()> = tokio::spawn(async move {
        let mut db_output_stream = db_output_stream;
        while let Some(vf_result) = db_output_stream.next().await {
            match vf_result {
                Message::Shutdown => {
                    if let Err(e) = needs_check_tx_clone.send(Message::Shutdown).await {
                        panic!("failed to send shutdown to needs check stream: {}", e);
                    }
                }
                Message::Data(Ok(vf)) => match vf.state {
                    Some(VirtualFileState::New)
                    | Some(VirtualFileState::Dirty)
                    | Some(VirtualFileState::Ok)
                    | Some(VirtualFileState::Deleted) => {
                        debug!("state -> splitter: {:?} ready for output", vf.path);
                        if let Err(e) = tx_clone.send(Ok(vf)).await {
                            panic!("failed to send error to output stream: {}", e);
                        }
                    }
                    Some(VirtualFileState::NeedsCheck) | None => {
                        debug!("state -> splitter: {:?} needs check", vf.path);
                        if needs_check_tx_clone.is_closed() {
                            panic!("programming error: data with needs check was received after shutdown");
                        }

                        if let Err(e) = needs_check_tx_clone.send(vf.into()).await {
                            if let Err(e_send) = tx_clone.send(Err(e.into())).await {
                                panic!("failed to send error to output stream: {}", e_send);
                            }
                        }
                    }
                },
                Message::Data(Err(e)) => {
                    if let Err(e_send) = tx_clone.send(Err(e.into())).await {
                        panic!("failed to send error to output stream: {}", e_send);
                    }
                }
            }
        }

        // close the channels once the stream is exhausted.
        drop(needs_check_tx_clone);
    });

    // performs the additional checks on files and then sends them back to the DB observation channel
    let vfs_clone = vfs.clone();
    let tx_clone = tx.clone();
    let obs_tx_clone = obs_tx.clone();
    let needs_check_tx_clone = needs_check_tx.clone();
    let checker_handle: JoinHandle<()> = tokio::spawn(async move {
        while let Some(vf_msg) = needs_check_rx.recv().await {
            match vf_msg {
                Message::Data(vf) => match check(&vfs_clone, vf).await {
                    Ok(observation) => {
                        if let Err(e) = obs_tx_clone.send(observation.into()).await {
                            if let Err(e_send) = tx_clone.send(Err(e.into())).await {
                                panic!("failed to send error to output stream: {}", e_send);
                            }
                        }
                    }
                    Err(e) => {
                        if let Err(e_send) = tx_clone.send(Err(e)).await {
                            panic!("failed to send error to output stream: {}", e_send);
                        }
                    }
                },
                Message::Shutdown => {
                    drop(needs_check_tx_clone);
                    break;
                }
            }
        }

        // Close the channels once the stream is exhausted.
        drop(obs_tx_clone);
    });

    let vfs_clone = vfs.clone();
    let tx_clone = tx.clone();
    let final_handle = tokio::spawn(async move {
        match tokio::try_join!(
            walker_handle,
            walker_transformer_handle,
            splitter_handle,
            checker_handle,
        ) {
            Ok(((), (), (), ())) => {
                close(tx_clone, vfs_clone, last_seen_id).await?;

                Ok(())
            }
            Err(e) => {
                error!("error in task execution: {}", e);
                Err(Error::TaskFailure(format!("task execution failed: {}", e)))
            }
        }
    });

    Ok((final_handle, ReceiverStream::new(rx)))
}