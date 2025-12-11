use crate::db::models;
use crate::db::models::{
    BlobID, Check, CurrentFile, FileCheck, FileSeen, MissingFile, VirtualFile as DBVirtualFile,
};
use crate::flightdeck;
use crate::flightdeck::base::BaseObserver;
use crate::flightdeck::tracer::Tracer;
use crate::flightdeck::tracked::sender;
use crate::repository::traits::{BufferType, Config, Local, VirtualFilesystem};
use crate::utils::blake3;
use crate::utils::errors::{AppError, InternalError};
use crate::utils::walker::{FileObservation, WalkerConfig, walk};
use futures::StreamExt;
use futures_core::stream::BoxStream;
use log::{debug, error};
use tokio::task::JoinHandle;

fn current_timestamp_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_nanos() as i64
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum VirtualFileState {
    New,
    Missing {
        target_blob_id: BlobID,
        local_has_target_blob: bool,
    },
    Ok {
        target_blob_id: BlobID,
    },
    OkMaterialisationMissing {
        target_blob_id: BlobID,
    },
    OkBlobMissing {
        target_blob_id: BlobID,
    },
    Altered {
        target_blob_id: BlobID,
        local_has_target_blob: bool,
    },
    Outdated {
        target_blob_id: Option<BlobID>,
        local_has_target_blob: bool,
    },
}

#[derive(Clone, Debug)]
pub struct VirtualFile {
    pub path: models::Path,
    pub state: VirtualFileState,
}

pub enum TryFromVirtualFileError {
    NeedsCheck {
        vf: models::VirtualFile,
    },
    CorruptionDetected {
        vf: models::VirtualFile,
        file: CurrentFile,
    },
}

impl TryFrom<models::VirtualFile> for VirtualFile {
    type Error = TryFromVirtualFileError;

    fn try_from(vf: models::VirtualFile) -> Result<Self, Self::Error> {
        match vf.state() {
            models::VirtualFileState::New => Ok(Self {
                path: vf.file_seen.path,
                state: VirtualFileState::New,
            }),
            models::VirtualFileState::Ok { file, blob: _ } => Ok(Self {
                path: vf.file_seen.path,
                state: VirtualFileState::Ok {
                    target_blob_id: file.blob_id,
                },
            }),
            models::VirtualFileState::OkMaterialisationMissing { file, blob: _ } => Ok(Self {
                path: vf.file_seen.path,
                state: VirtualFileState::OkMaterialisationMissing {
                    target_blob_id: file.blob_id,
                },
            }),
            models::VirtualFileState::OkBlobMissing { file } => Ok(Self {
                path: vf.file_seen.path,
                state: VirtualFileState::OkBlobMissing {
                    target_blob_id: file.blob_id,
                },
            }),
            models::VirtualFileState::Altered { file, blob } => Ok(Self {
                path: vf.file_seen.path,
                state: VirtualFileState::Altered {
                    target_blob_id: file.blob_id,
                    local_has_target_blob: blob.is_some(),
                },
            }),
            models::VirtualFileState::Outdated { file, blob, mat: _ } => Ok(Self {
                path: vf.file_seen.path,
                state: VirtualFileState::Outdated {
                    target_blob_id: file.map(|f| f.blob_id),
                    local_has_target_blob: blob.is_some(),
                },
            }),
            models::VirtualFileState::NeedsCheck => Err(TryFromVirtualFileError::NeedsCheck { vf }),
            models::VirtualFileState::CorruptionDetected { file, blob: _ } => {
                Err(TryFromVirtualFileError::CorruptionDetected { vf, file })
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
async fn check(vfs: &impl Local, vf: &models::VirtualFile) -> Result<FileCheck, InternalError> {
    let path = vfs.root().join(vf.file_seen.path.0.clone());

    let blake3::HashWithSize { hash, .. } = blake3::compute_blake3_and_size(&path).await?;

    Ok(FileCheck {
        path: vf.file_seen.path.clone(),
        check_dttm: chrono::Utc::now(),
        hash,
    })
}

async fn close(
    tx: sender::TrackedSender<Result<VirtualFile, InternalError>, sender::Adapter>,
    vfs: impl VirtualFilesystem + Local + Send + Sync + Clone,
    last_seen_id: i64,
) -> Result<(), InternalError> {
    debug!("close: closing virtual filesystem");

    let tracer = Tracer::new_on("state::close");

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

    tracer.measure();

    Ok(())
}

pub async fn state(
    vfs: impl VirtualFilesystem + Local + Config + Send + Sync + Clone + 'static,
    config: WalkerConfig,
) -> Result<
    (
        JoinHandle<Result<(), InternalError>>,
        BoxStream<'static, Result<VirtualFile, InternalError>>,
    ),
    InternalError,
> {
    let vfs_clone = vfs.clone();

    let (bg, s, fc) = state_with_checks(vfs, config).await?;

    let bg = tokio::spawn(async move {
        vfs_clone.add_checked_events(fc).await?;
        drop(vfs_clone);

        let tracer = Tracer::new_on("state::bg::await");
        let result = bg.await?;
        tracer.measure();
        result
    });

    Ok((bg, s))
}

pub async fn state_with_checks(
    vfs: impl VirtualFilesystem + Local + Config + Send + Sync + Clone + 'static,
    config: WalkerConfig,
) -> Result<
    (
        JoinHandle<Result<(), InternalError>>,
        BoxStream<'static, Result<VirtualFile, InternalError>>,
        BoxStream<'static, FileCheck>,
    ),
    InternalError,
> {
    let root = vfs.root();
    let last_seen_id = current_timestamp_ns();

    let mut vfs_obs = BaseObserver::without_id("vfs:refresh");
    vfs_obs.observe_state(log::Level::Debug, "refreshing virtual filesystem...");
    let start_time = tokio::time::Instant::now();
    let duration = start_time.elapsed();
    vfs_obs.observe_termination(
        log::Level::Debug,
        format!("refreshed virtual filesystem in {:.2?}", duration),
    );

    let buffer_size = vfs.buffer_size(BufferType::StateBufferChannelSize);
    let checker_buffer_size = vfs.buffer_size(BufferType::StateCheckerParallelism);
    let (seen_tx, seen_rx) = flightdeck::tracked::mpsc_channel("state::seen", buffer_size);
    let (seen_sink_tx, seen_db_rx) =
        flightdeck::tracked::mpsc_channel("state::seen:persist", buffer_size);
    let (check_sink_tx, check_db_rx) =
        flightdeck::tracked::mpsc_channel("state::check:persist", buffer_size);
    let (needs_check_tx, needs_check_rx) =
        flightdeck::tracked::mpsc_channel("state::needs_check", buffer_size);
    let (tx, rx) = flightdeck::tracked::mpsc_channel("state", buffer_size);

    // sinks the observation channel with the DB
    let vfs_clone = vfs.clone();
    let tx_clone = tx.clone();
    let bg_seen_persist: JoinHandle<()> = tokio::spawn(async move {
        if let Err(e) = vfs_clone.add_seen_events(seen_db_rx.boxed()).await {
            tx_clone
                .send(Err(e.into()))
                .await
                .expect("failed to send error to output stream");
        }
    });

    // enrich and split walker stream
    let walker_buffer_size = vfs.buffer_size(BufferType::WalkerChannelSize);
    let (bg_walker, walker_rx) = walk(root.abs().clone(), config, walker_buffer_size)
        .await
        .inspect_err(|e| log::error!("state: walk failed: {e}"))?;
    let walker_rx = walker_rx.map(move |e| {
        e.map(|fo: FileObservation| FileSeen {
            path: models::Path(fo.rel_path.to_string_lossy().to_string()),
            last_modified_dttm: fo.last_modified,
            size: fo.size,
            seen_id: last_seen_id,
            seen_dttm: chrono::Utc::now(),
        })
    });
    let tx_clone = tx.clone();
    let bg_split_seen: JoinHandle<()> = tokio::spawn(async move {
        let mut walker_rx = walker_rx;
        while let Some(fs) = walker_rx.next().await {
            match fs {
                Ok(fs) => {
                    if let Err(e) = seen_tx.send(fs.clone()).await {
                        tx_clone
                            .send(Err(e.into()))
                            .await
                            .expect("failed to send error to output stream");
                    }
                    if let Err(e) = seen_sink_tx.send(fs).await {
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
    });

    // enrich file seen stream & split it
    let vfs_clone = vfs.clone();
    let tx_clone = tx.clone();
    let bg_split_vfs: JoinHandle<()> = tokio::spawn(async move {
        let mut vfs_stream = vfs_clone
            .select_virtual_filesystem(seen_rx.boxed())
            .await
            .boxed();
        while let Some(vf) = vfs_stream.next().await {
            match vf {
                Ok(vf) => {
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
                            TryFromVirtualFileError::NeedsCheck { vf } => {
                                debug!("state -> splitter: {:?} needs check", vf);
                                if let Err(e) = needs_check_tx.send(vf).await {
                                    tx_clone
                                        .send(Err(e.into()))
                                        .await
                                        .expect("failed to send error to output stream")
                                }
                            }
                            TryFromVirtualFileError::CorruptionDetected { vf, file } => {
                                debug!("corruption detected: {:?}", vf);
                                tx_clone
                                    .send(Err(AppError::CorruptionDetected {
                                        blob_id: file.blob_id,
                                        path: vf.file_seen.path,
                                    }
                                    .into()))
                                    .await
                                    .expect("failed to send error to output stream")
                            }
                        },
                    }
                }
                Err(e) => tx_clone
                    .send(Err(e.into()))
                    .await
                    .expect("failed to send error to output stream"),
            }
        }
    });

    // check recently altered files
    let vfs_clone = vfs.clone();
    let tx_clone = tx.clone();
    let bg_check: JoinHandle<()> = tokio::spawn(async move {
        needs_check_rx
            .map(|vf| {
                let vfs_clone = vfs_clone.clone();
                async move {
                    match check(&vfs_clone, &vf).await {
                        Ok(check) => Ok((vf, check)),
                        Err(e) => Err(e),
                    }
                }
            })
            .buffer_unordered(checker_buffer_size)
            .for_each(|result| async {
                match result {
                    Ok((vf, check)) => {
                        if let Err(e) = check_sink_tx.send(check.clone()).await {
                            tx_clone
                                .send(Err(e.into()))
                                .await
                                .expect("failed to send error to output stream")
                        }

                        let vf = DBVirtualFile {
                            file_seen: vf.file_seen,
                            current_file: vf.current_file,
                            current_blob: vf.current_blob,
                            current_materialisation: vf.current_materialisation,
                            current_check: Some(Check {
                                check_last_dttm: check.check_dttm,
                                check_last_hash: check.hash,
                            }),
                        };

                        let vf: Result<VirtualFile, _> = vf.try_into();
                        match vf {
                            Ok(vf) => {
                                debug!("state -> post-check: {:?} ready for output", vf.path);
                                tx_clone
                                    .send(Ok(vf))
                                    .await
                                    .expect("failed to send error to output stream")
                            }
                            Err(vf) => match vf {
                                TryFromVirtualFileError::NeedsCheck { vf } => {
                                    debug!(
                                        "state -> post-check: {:?} needs check again (error)",
                                        vf
                                    );
                                    let e = AppError::FileStateCannotBeDetermined {
                                        path: vf.file_seen.path,
                                    };
                                    tx_clone
                                        .send(Err(e.into()))
                                        .await
                                        .expect("failed to send error to output stream")
                                }
                                TryFromVirtualFileError::CorruptionDetected { vf, file } => {
                                    debug!("corruption detected: {:?}", vf);
                                    tx_clone
                                        .send(Err(AppError::CorruptionDetected {
                                            blob_id: file.blob_id,
                                            path: vf.file_seen.path,
                                        }
                                        .into()))
                                        .await
                                        .expect("failed to send error to output stream")
                                }
                            },
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
    });

    let vfs_clone = vfs.clone();
    let tx_clone = tx.clone();
    let bg_final = tokio::spawn(async move {
        match tokio::try_join!(
            bg_walker,
            bg_split_seen,
            bg_split_vfs,
            bg_check,
            bg_seen_persist
        ) {
            Ok(((), (), (), (), ())) => {
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

    Ok((bg_final, rx.boxed(), check_db_rx.boxed()))
}
