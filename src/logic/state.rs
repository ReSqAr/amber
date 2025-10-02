use crate::db::models;
use crate::db::models::{FileCheck, FileSeen, MissingFile, Observation};
use crate::flightdeck;
use crate::flightdeck::base::BaseObserver;
use crate::flightdeck::tracked::sender;
use crate::repository::traits::{BufferType, Config, Local, VirtualFilesystem};
use crate::utils;
use crate::utils::errors::{AppError, InternalError};
use crate::utils::flow::{ExtFlow, Flow};
use crate::utils::sha256;
use crate::utils::walker::{FileObservation, WalkerConfig, walk};
use futures::{Stream, StreamExt, future};
use log::{debug, error};
use tokio::task::JoinHandle;
use utils::fs::are_hardlinked;

fn current_timestamp_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_nanos() as i64
}

async fn check(vfs: &impl Local, vf: models::VirtualFile) -> Result<Observation, InternalError> {
    let path = vfs.root().join(vf.path.clone());

    let hash = if let Some(blob_id) = vf.materialisation_last_blob_id.clone() {
        if vf.local_has_target_blob && are_hardlinked(&path, &vfs.blob_path(&blob_id)).await? {
            Some(blob_id)
        } else {
            None
        }
    } else {
        None
    };

    // fallback
    let hash = if let Some(hash) = hash {
        hash
    } else {
        let sha256::HashWithSize { hash, .. } = sha256::compute_sha256_and_size(&path).await?;
        hash
    };

    Ok(Observation::FileCheck(FileCheck {
        path: vf.path.clone(),
        check_dttm: current_timestamp_ns(),
        hash,
    }))
}

async fn close(
    tx: sender::TrackedSender<Result<VirtualFile, InternalError>, sender::Adapter>,
    vfs: impl VirtualFilesystem + Local + Send + Sync + Clone,
    last_seen_id: i64,
) -> Result<(), InternalError> {
    debug!("close: closing virtual filesystem");

    let start_time = tokio::time::Instant::now();
    vfs.cleanup(last_seen_id).await?;
    let duration = start_time.elapsed();
    debug!("cleanup: {:.2?}", duration);

    let start_time = tokio::time::Instant::now();

    let mut deleted_files = vfs.select_missing_files(last_seen_id).await;
    while let Some(r) = deleted_files.next().await {
        match r {
            Ok(vf) => tx
                .send(Ok(vf.into()))
                .await
                .expect("failed to send element to output stream"),
            Err(e) => tx
                .send(Err(e.into()))
                .await
                .expect("failed to send error to output stream"),
        }
    }

    let duration = start_time.elapsed();
    debug!("deleted: {:.2?}", duration);

    Ok(())
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum VirtualFileState {
    New,
    Missing {
        target_blob_id: String,
        local_has_target_blob: bool,
    },
    Ok {
        target_blob_id: String,
        local_has_target_blob: bool,
    },
    OkMaterialisationMissing {
        target_blob_id: Option<String>,
        local_has_target_blob: bool,
    },
    Altered {
        target_blob_id: Option<String>,
        local_has_target_blob: bool,
    },
    Outdated {
        target_blob_id: Option<String>,
        local_has_target_blob: bool,
    },
}

#[derive(Clone, Debug)]
pub struct VirtualFile {
    pub path: String,
    pub state: VirtualFileState,
}

pub enum TryFromVirtualFileError {
    NeedsCheck(models::VirtualFile),
    CorruptionDetected(models::VirtualFile),
}

impl TryFrom<models::VirtualFile> for VirtualFile {
    type Error = TryFromVirtualFileError;

    fn try_from(vf: models::VirtualFile) -> Result<Self, Self::Error> {
        match vf.state {
            models::VirtualFileState::New => Ok(Self {
                path: vf.path,
                state: VirtualFileState::New,
            }),
            models::VirtualFileState::Ok => Ok(Self {
                path: vf.path,
                state: VirtualFileState::Ok {
                    target_blob_id: vf.target_blob_id.unwrap(),
                    local_has_target_blob: vf.local_has_target_blob,
                },
            }),
            models::VirtualFileState::OkMaterialisationMissing => Ok(Self {
                path: vf.path,
                state: VirtualFileState::OkMaterialisationMissing {
                    target_blob_id: vf.target_blob_id,
                    local_has_target_blob: vf.local_has_target_blob,
                },
            }),
            models::VirtualFileState::Altered => Ok(Self {
                path: vf.path,
                state: VirtualFileState::Altered {
                    target_blob_id: vf.target_blob_id,
                    local_has_target_blob: vf.local_has_target_blob,
                },
            }),
            models::VirtualFileState::Outdated => Ok(Self {
                path: vf.path,
                state: VirtualFileState::Outdated {
                    target_blob_id: vf.target_blob_id,
                    local_has_target_blob: vf.local_has_target_blob,
                },
            }),
            models::VirtualFileState::NeedsCheck => Err(TryFromVirtualFileError::NeedsCheck(vf)),
            models::VirtualFileState::CorruptionDetected => {
                Err(TryFromVirtualFileError::CorruptionDetected(vf))
            }
        }
    }
}

impl From<MissingFile> for VirtualFile {
    fn from(vf: MissingFile) -> Self {
        Self {
            path: vf.path,
            state: VirtualFileState::Missing {
                target_blob_id: vf.target_blob_id,
                local_has_target_blob: vf.local_has_target_blob,
            },
        }
    }
}

pub async fn state(
    vfs: impl VirtualFilesystem + Local + Config + Send + Sync + Clone + 'static,
    config: WalkerConfig,
) -> Result<
    (
        JoinHandle<Result<(), InternalError>>,
        impl Stream<Item = Result<VirtualFile, InternalError>>,
    ),
    InternalError,
> {
    let root = vfs.root();
    let last_seen_id = current_timestamp_ns();

    let mut vfs_obs = BaseObserver::without_id("vfs:refresh");
    vfs_obs.observe_state(log::Level::Debug, "refreshing virtual filesystem...");
    let start_time = tokio::time::Instant::now();
    vfs.refresh()
        .await
        .inspect_err(|e| log::error!("state: vfs refresh failed: {e}"))?;
    let duration = start_time.elapsed();
    vfs_obs.observe_termination(
        log::Level::Debug,
        format!("refreshed virtual filesystem in {:.2?}", duration),
    );

    /* Three channels:
       - obs: observations by the filesystem walker
       - needs_check: files which need to be looked at
       - output channel
    */
    let buffer_size = vfs.buffer_size(BufferType::StateBufferChannelSize);
    let checker_buffer_size = vfs.buffer_size(BufferType::StateCheckerParallelism);
    let (obs_tx, obs_rx) = flightdeck::tracked::mpsc_channel("state::obs", buffer_size);
    let (needs_check_tx, needs_check_rx) =
        flightdeck::tracked::mpsc_channel("state::needs_check", buffer_size);
    let (tx, rx) = flightdeck::tracked::mpsc_channel("state", buffer_size);

    // get the channel of the filesystem walker
    let (walker_handle, mut walker_rx) = walk(
        root.abs().clone(),
        config,
        vfs.buffer_size(BufferType::WalkerChannelSize),
    )
    .await
    .inspect_err(|e| log::error!("state: walk failed: {e}"))?;

    // connects the observation channel with the DB and receives the results via the db output stream
    let db_output_stream = vfs.add_observations(obs_rx).await;

    // transforms the files seen by the filesystem walker into observations suitable for the DB
    let tx_clone = tx.clone();
    let obs_tx_clone = obs_tx.clone();
    let walker_transformer_handle: JoinHandle<()> = tokio::spawn(async move {
        while let Some(file_result) = walker_rx.next().await {
            match file_result {
                Ok(FileObservation {
                    rel_path,
                    size,
                    last_modified_ns,
                }) => {
                    let observation = Observation::FileSeen(FileSeen {
                        path: rel_path.to_string_lossy().to_string(),
                        seen_id: last_seen_id,
                        seen_dttm: current_timestamp_ns(),
                        last_modified_dttm: last_modified_ns,
                        size,
                    });

                    // we have a loop in the data flow. this code gives the checker
                    // observation stream priority. otherwise we run into deadlocks
                    let max_delay = tokio::time::Duration::from_millis(10);
                    let mut current_delay = tokio::time::Duration::from_millis(1);
                    loop {
                        if obs_tx_clone.capacity() > obs_tx_clone.max_capacity() / 2 {
                            break;
                        }
                        tokio::time::sleep(current_delay).await;
                        current_delay = std::cmp::min(current_delay * 2, max_delay);
                    }

                    if let Err(e) = obs_tx_clone.send(observation.into()).await {
                        tx_clone
                            .send(Err(e.into()))
                            .await
                            .expect("failed to send error to output stream");
                    }
                }
                Err(e) => tx_clone
                    .send(Err(e.into()))
                    .await
                    .expect("failed to send error to output stream"),
            }
        }

        // close the channels once the stream is exhausted.
        obs_tx_clone
            .send(Flow::Shutdown)
            .await
            .expect("failed to send error to output stream")
    });

    // splits the observations by the filesystem walker into:
    // - output channel: for states new/dirty/ok
    // - needs_check channel: needs_check or unknown
    let tx_clone = tx.clone();
    let needs_check_tx_clone = needs_check_tx.clone();
    let db_output_stream = db_output_stream.boxed();
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
                        let vf: Result<VirtualFile, _> = vf.try_into();
                        match vf {
                            Ok(vf) => {
                                debug!("state -> splitter: {:?} ready for output", vf.path);
                                tx_clone
                                    .send(Ok(vf))
                                    .await
                                    .expect("failed to send error to output stream")
                            }
                            Err(vf) => match vf {
                                TryFromVirtualFileError::NeedsCheck(vf) => {
                                    debug!("state -> splitter: {:?} needs check", vf.path);
                                    if needs_check_tx_clone.is_closed() {
                                        panic!(
                                            "programming error: data with needs check was received after shutdown"
                                        );
                                    }

                                    if let Err(e) =
                                        needs_check_tx_clone.send(vf.clone().into()).await
                                    {
                                        tx_clone
                                            .send(Err(e.into()))
                                            .await
                                            .expect("failed to send error to output stream")
                                    }
                                }
                                TryFromVirtualFileError::CorruptionDetected(vf) => {
                                    debug!("corruption detected: {:?}", vf);
                                    tx_clone
                                        .send(Err(AppError::CorruptionDetected {
                                            blob_id: vf.target_blob_id,
                                            path: vf.path,
                                        }
                                        .into()))
                                        .await
                                        .expect("failed to send error to output stream")
                                }
                            },
                        }
                    }
                }
                Err(e) => tx_clone
                    .send(Err(e.into()))
                    .await
                    .expect("failed to send error to output stream"),
            }
            if has_shutdown {
                needs_check_tx_clone
                    .send(Flow::Shutdown)
                    .await
                    .expect("failed to send shutdown to needs check stream")
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
        needs_check_rx
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
            .buffer_unordered(checker_buffer_size)
            .for_each(|result| async {
                match result {
                    Ok(observation) => {
                        if let Err(e) = obs_tx_clone.send(observation.into()).await {
                            tx_clone
                                .send(Err(e.into()))
                                .await
                                .expect("failed to send error to output stream")
                        }
                    }
                    Err(e) => {
                        log::error!("state check error: {:?}", e);
                        tx_clone
                            .send(Err(e))
                            .await
                            .expect("failed to send error to output stream")
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
                close(tx_clone, vfs_clone, last_seen_id)
                    .await
                    .inspect_err(|e| log::error!("state: close failed: {e}"))?;

                Ok(())
            }
            Err(e) => {
                error!("error in task execution: {e}");
                Err(InternalError::TaskFailure(format!(
                    "task execution failed: {e}"
                )))
            }
        }
    });

    Ok((final_handle, rx))
}
