use crate::db::models::{FileEqBlobCheck, FileSeen, Observation, VirtualFile, VirtualFileState};
use crate::repository::traits::{Local, VirtualFilesystem};
use crate::utils;
use crate::utils::flow::{ExtFlow, Flow};
use crate::utils::walker;
use crate::utils::walker::{walk, FileObservation, WalkerConfig};
use futures::{future, Stream, StreamExt};
use log::{debug, error};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use utils::fs::are_hardlinked;

pub struct StateConfig {
    buffer_size: usize,
    walker: WalkerConfig,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            buffer_size: 10000,
            walker: WalkerConfig::default(),
        }
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("fs walker error: {0}")]
    Walker(#[from] walker::Error),
    #[error("observation send error: {0}")]
    Send(String),
    #[error("task execution failed: {0}")]
    TaskFailure(String),
}

impl<T> From<SendError<T>> for Error {
    fn from(value: SendError<T>) -> Self {
        Error::Send(value.to_string())
    }
}

fn current_timestamp() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_secs() as i64
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

    let start_time = Instant::now();
    vfs.cleanup(last_seen_id).await?;
    let duration = start_time.elapsed();
    debug!("cleanup: {:.2?}", duration);

    let start_time = Instant::now();

    let mut deleted_files = vfs.select_deleted_files(last_seen_id).await;
    while let Some(r) = deleted_files.next().await {
        match r {
            Ok(vf) => {
                if let Err(e) = tx.send(Ok(vf)).await {
                    panic!("failed to send element to output stream: {e}");
                }
            }
            Err(e) => {
                if let Err(e_send) = tx.send(Err(e.into())).await {
                    panic!("failed to send error to output stream: {}", e_send);
                }
            }
        }
    }

    let duration = start_time.elapsed();
    debug!("deleted: {:.2?}", duration);

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

    let start_time = Instant::now();
    vfs.refresh().await?;
    debug!("virtual filesystem refreshed");
    let duration = start_time.elapsed();
    debug!("refreshed: {:.2?}", duration);

    /* Three channels:
       - obs: observations by the filesystem walker
       - needs_check: files which need to be looked at
       - output channel
    */
    let (obs_tx, obs_rx) = mpsc::channel(config.buffer_size);
    let (needs_check_tx, needs_check_rx) = mpsc::channel(config.buffer_size);
    let (tx, rx) = mpsc::channel(config.buffer_size);

    // get the channel of the filesystem walker
    let (walker_handle, mut walker_rx) = walk(root.abs().clone(), config.walker).await?;

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
        if let Err(e) = obs_tx_clone.send(Flow::Shutdown).await {
            panic!("failed to send error to output stream: {e}");
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
            let (has_shutdown, data) = match vf_result {
                ExtFlow::Data(data) => (false, data),
                ExtFlow::Shutdown(data) => (true, data),
            };
            match data {
                Ok(vfs) => {
                    for vf in vfs {
                        match vf.state {
                            Some(VirtualFileState::New)
                            | Some(VirtualFileState::Dirty)
                            | Some(VirtualFileState::Ok)
                            | Some(VirtualFileState::Deleted) => {
                                debug!("state -> splitter: {:?} ready for output", vf.path);
                                if let Err(e) = tx_clone.send(Ok(vf)).await {
                                    panic!("failed to send error to output stream: {e}");
                                }
                            }
                            Some(VirtualFileState::NeedsCheck) | None => {
                                debug!("state -> splitter: {:?} needs check", vf.path);
                                if needs_check_tx_clone.is_closed() {
                                    panic!("programming error: data with needs check was received after shutdown");
                                }

                                if let Err(e) = needs_check_tx_clone.send(vf.clone().into()).await {
                                    if let Err(e_send) = tx_clone.send(Err(e.into())).await {
                                        panic!("failed to send error to output stream: {}", e_send);
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    if let Err(e_send) = tx_clone.send(Err(e.into())).await {
                        panic!("failed to send error to output stream: {}", e_send);
                    }
                }
            }
            if has_shutdown {
                if let Err(e) = needs_check_tx_clone.send(Flow::Shutdown).await {
                    panic!("failed to send shutdown to needs check stream: {e}");
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
        ReceiverStream::new(needs_check_rx)
            .take_while(|message| future::ready(matches!(message, Flow::Data(_))))
            .filter_map(|message| {
                future::ready(match message {
                    Flow::Data(x) => Some(x),
                    _ => None, // This branch won't actually be hit due to take_while
                })
            })
            .map(|vf| {
                let vfs_clone = vfs_clone.clone();
                async move {
                    match check(&vfs_clone, vf).await {
                        Ok(observation) => Ok(observation),
                        Err(e) => Err(e),
                    }
                }
            })
            .buffer_unordered(config.buffer_size)
            .for_each(|result| async {
                match result {
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
                }
            })
            .await;

        // close the channels once the stream is exhausted.
        drop(needs_check_tx_clone); // note: this is how we break the dependency loop
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
                error!("error in task execution: {e}");
                Err(Error::TaskFailure(format!("task execution failed: {e}")))
            }
        }
    });

    Ok((final_handle, ReceiverStream::new(rx)))
}
