use crate::repository::local_repository::LocalRepository;
use crate::repository::traits::{Local, VirtualFilesystem};
use crate::utils::walker::{walk, FileObservation, WalkerConfig};
use log::{error, info};

use crate::db::models::{FileEqBlobCheck, FileSeen, FileState, Observation, VirtualFile};
use crate::utils::walker;
use futures::{Stream, StreamExt};
use std::fmt::Debug;
use std::path::PathBuf;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

pub async fn status() -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    let (handle, mut stream) = state(
        local_repository.root(),
        local_repository,
        StateConfig::default(),
    )
    .await?;
    let output_handle = tokio::spawn(async move {
        while let Some(file_result) = stream.next().await {
            match file_result {
                Ok(file_obs) => {
                    info!("file observed: {:?}", file_obs);
                }
                Err(e) => {
                    error!("Error during traversal: {}", e);
                }
            }
        }
    });

    handle.await??;
    output_handle.await?;

    Ok(())
}

fn current_timestamp() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_secs() as i64
}

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

pub async fn state(
    root: PathBuf,
    db: impl VirtualFilesystem,
    config: StateConfig,
) -> Result<
    (
        JoinHandle<Result<(), Error>>,
        impl Stream<Item = Result<VirtualFile, Error>>,
    ),
    Box<dyn std::error::Error>,
> {
    let last_seen_id = current_timestamp();

    db.refresh().await?;
    info!("Virtual filesystem refreshed.");

    /* Three channels:
       - obs: observations by the filesystem walker
       - needs_check: files which need to be looked at
       - output channel
    */
    let (obs_tx, obs_rx) = mpsc::channel::<Observation>(config.buffer_size);
    let (needs_check_tx, mut needs_check_rx): (Sender<VirtualFile>, Receiver<VirtualFile>) =
        mpsc::channel(config.buffer_size);
    let (tx, rx): (
        Sender<Result<VirtualFile, Error>>,
        Receiver<Result<VirtualFile, Error>>,
    ) = mpsc::channel(config.buffer_size);

    // get the channel of the filesystem walker
    let (walker_handle, mut walker_rx) = walk(root, config.walker).await?;

    // connects the observation channel with the DB and receives the results via the db output stream
    let db_output_rx = db.add_observations(ReceiverStream::new(obs_rx)).await;

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
                    if let Err(e) = obs_tx_clone.send(observation).await {
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
        // TODO: signal to obs_tx_clone that no more data will come
    });

    // splits the observations by the filesystem walker into:
    // - output channel: for states new/dirty/ok
    // - needs_check channel: needs_check or unknown
    let tx_clone = tx.clone();
    let splitter_handle: JoinHandle<()> = tokio::spawn(async move {
        let mut db_output_stream = db_output_rx;
        while let Some(vf_result) = db_output_stream.next().await {
            match vf_result {
                Ok(vf) => match vf.state {
                    Some(FileState::New) | Some(FileState::Dirty) | Some(FileState::Ok) => {
                        if let Err(e) = tx_clone.send(Ok(vf)).await {
                            panic!("failed to send error to output stream: {}", e);
                        }
                    }
                    Some(FileState::NeedsCheck) | None => {
                        if let Err(e) = needs_check_tx.send(vf).await {
                            if let Err(e_send) = tx_clone.send(Err(e.into())).await {
                                panic!("failed to send error to output stream: {}", e_send);
                            }
                        }
                    }
                },
                Err(e) => {
                    if let Err(e_send) = tx_clone.send(Err(e.into())).await {
                        panic!("failed to send error to output stream: {}", e_send);
                    }
                }
            }
        }

        // Close the channels once the stream is exhausted.
        drop(needs_check_tx);
    });

    // performs the additional checks on files and then sends them back to the DB observation channel
    let obs_tx_clone = obs_tx.clone();
    let tx_clone = tx.clone();
    let checker_handle: JoinHandle<()> = tokio::spawn(async move {
        while let Some(vf) = needs_check_rx.recv().await {
            // TODO: checker
            let observation = Observation::FileEqBlobCheck(FileEqBlobCheck {
                path: vf.path.clone(),
                last_check_dttm: current_timestamp(),
                last_result: true,
            });
            if let Err(e) = obs_tx_clone.send(observation).await {
                if let Err(e_send) = tx_clone.send(Err(e.into())).await {
                    panic!("failed to send error to output stream: {}", e_send);
                }
            }
        }

        // Close the channels once the stream is exhausted.
        drop(obs_tx_clone);
    });

    let final_handle = tokio::spawn(async move {
        match tokio::try_join!(
            walker_handle,
            walker_transformer_handle,
            splitter_handle,
            checker_handle,
        ) {
            Ok(((), (), (), ())) => Ok(()),
            Err(e) => {
                error!("error in task execution: {}", e);
                Err(Error::TaskFailure(format!("task execution failed: {}", e)))
            }
        }
    });

    Ok((final_handle, ReceiverStream::new(rx)))
}
