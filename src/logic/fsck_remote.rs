use crate::connection::EstablishedConnection;
use crate::db::models::{AvailableBlob, InsertBlob, RepoID, SizedBlobID};
use crate::db::{kvstore, models};
use crate::flightdeck::base::BaseObserver;
use crate::flightdeck::base::{BaseObservable, BaseObservation};
use crate::flightdeck::observer::Observer;
use crate::flightdeck::tracked::stream::Trackable;
use crate::logic::files;
use crate::repository::traits::{
    Adder, Availability, BufferType, Config, Local, Metadata, RcloneTargetPath,
};
use crate::utils::errors::InternalError;
use crate::utils::pipe::TryForwardIntoExt;
use crate::utils::rclone::{
    Operation, RCloneConfig, RCloneTarget, RcloneEvent, RcloneStats, run_rclone,
};
use crate::utils::units;
use chrono::{DateTime, Utc};
use rand::Rng;
use redb::TableDefinition;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::{fs, time};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

#[derive(Debug, Clone)]
pub struct ObservedBlob {
    pub repo_id: RepoID,
    pub has_blob: bool,
    pub path: models::Path,
    pub valid_from: DateTime<Utc>,
}

pub(crate) async fn fsck_remote(
    local: &(impl Config + Local + Adder + Sync + Send + Clone + 'static),
    remote: &(impl Metadata + Availability + RcloneTargetPath),
    connection: &EstablishedConnection,
    config: RCloneConfig,
) -> Result<(), InternalError> {
    let staging_id: u32 = rand::rng().random();
    let fsck_path = local.staging_id_path(staging_id);
    let fsck_files_path = local.rclone_target_path(staging_id);
    let start_time = tokio::time::Instant::now();
    fs::create_dir_all(&fsck_path).await?;
    let rclone_files = fsck_path.join("rclone.files");
    let redb = kvstore::KVStore::<
        models::Path,
        models::SizedBlobID,
        models::Path,
        models::SizedBlobID,
    >::new(
        fsck_path.join("scratch.redb").abs().to_owned(),
        TableDefinition::new("scratch"),
    )
    .await?;
    let rclone_source =
        connection.local_rclone_target(fsck_files_path.abs().to_string_lossy().into());
    let rclone_destination = connection.remote_rclone_target(remote.rclone_path(staging_id).await?);

    let expected_count = {
        let (writing_task, tx) =
            write_rclone_files_fsck_clone(local, rclone_files.clone().abs().clone());

        let mut obs = BaseObserver::without_id("fsck:files:materialise");
        let stream = remote.available();
        let local_clone = local.clone();
        let fsck_files_path_clone = fsck_files_path.clone();
        let skipped = Arc::new(AtomicU64::new(0));
        let skipped_clone = skipped.clone();
        let count = Arc::new(AtomicU64::new(0));
        let count_clone = count.clone();
        let obs_clone = obs.clone();
        let stream = StreamExt::map(stream, move |blob: Result<AvailableBlob, InternalError>| {
            let local = local_clone.clone();
            let fsck_files_path = fsck_files_path_clone.clone();
            let tx = tx.clone();
            let skipped = skipped_clone.clone();
            let count = count_clone.clone();
            let mut obs = obs_clone.clone();
            async move {
                let blob = blob?;
                let blob_path = local.blob_path(&blob.blob_id);
                let mut o = BaseObserver::with_id("fsck:file:materialise", blob.blob_id.0.clone());

                if fs::metadata(&blob_path)
                    .await
                    .map(|m| m.is_file())
                    .unwrap_or(false)
                {
                    if let Some(path) = blob.path {
                        files::create_link(
                            &blob_path,
                            &fsck_files_path.join(&path),
                            local.capability(),
                        )
                        .await?;
                        tx.send(path.clone()).await?;
                        o.observe_termination(log::Level::Debug, "materialised");
                        let new_count = count.fetch_add(1, Ordering::Relaxed) + 1;
                        obs.observe_position(log::Level::Trace, new_count);
                        Ok::<Option<(models::Path, SizedBlobID)>, InternalError>(Some((
                            models::Path(path),
                            SizedBlobID {
                                blob_id: blob.blob_id,
                                blob_size: blob.blob_size,
                            },
                        )))
                    } else {
                        o.observe_termination(log::Level::Debug, "skipped (orphaned)");
                        skipped.fetch_add(1, Ordering::Relaxed);
                        Ok::<_, InternalError>(None)
                    }
                } else {
                    o.observe_termination(log::Level::Info, "skipped (blob missing)");
                    skipped.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, InternalError>(None)
                }
            }
        });

        let stream = futures::StreamExt::buffer_unordered(
            stream,
            local.buffer_size(BufferType::FsckMaterialiseBufferParallelism),
        );
        let stream = StreamExt::filter_map(stream, Result::transpose);
        let stream = StreamExt::map(stream, |e| e.map(|(p, s)| (p, Some(s))));
        redb.apply(futures::StreamExt::boxed(stream)).await?;

        writing_task.await??;

        let duration = start_time.elapsed();
        let count = count.load(Ordering::Relaxed);
        let mut parts = vec![format!("materialised {count} blobs")];
        let skipped = skipped.load(Ordering::SeqCst);
        if skipped > 0 {
            parts.push(format!("skipped {} blobs", skipped));
        }
        let msg = format!("{} for fsck in {duration:.2?}", parts.join(" and "));
        obs.observe_termination(log::Level::Info, msg);

        count
    };

    let (tx, rx) = mpsc::unbounded_channel::<RCloneResult>();
    let local_clone = local.clone();
    let repo_id = remote.current().await?.id;
    let result_handler = tokio::spawn(async move {
        let s = UnboundedReceiverStream::new(rx).track("fsck_remote::rx");
        let s = StreamExt::map(s, move |r: RCloneResult| match r {
            RCloneResult::Success(object) => {
                BaseObserver::with_id("rclone:check", object.clone())
                    .observe_termination(log::Level::Debug, "OK");
                Ok(ObservedBlob {
                    repo_id: repo_id.clone(),
                    has_blob: true,
                    path: models::Path(object),
                    valid_from: chrono::Utc::now(),
                })
            }
            RCloneResult::Failure(object) => {
                BaseObserver::with_id("rclone:check", object.clone())
                    .observe_termination(log::Level::Error, "altered");
                Ok(ObservedBlob {
                    repo_id: repo_id.clone(),
                    has_blob: false,
                    path: models::Path(object),
                    valid_from: chrono::Utc::now(),
                })
            }
        });
        let s = redb
            .left_join::<_, _, InternalError>(futures::StreamExt::boxed(s), |e: ObservedBlob| {
                e.path
            });
        let s = StreamExt::map(s, |r| {
            r.map(|(o, b)| {
                let b = b.unwrap();
                InsertBlob {
                    repo_id: o.repo_id,
                    blob_id: b.blob_id,
                    blob_size: b.blob_size,
                    has_blob: o.has_blob,
                    path: Some(o.path),
                    valid_from: o.valid_from,
                }
            })
        });
        s.try_forward_into::<_, _, _, _, InternalError>(|s| {
            local_clone.add_blobs(futures::StreamExt::boxed(s))
        })
        .await?;

        redb.close().await?;

        Ok::<(), InternalError>(())
    });

    execute_rclone(
        &fsck_path.abs().clone(),
        rclone_source,
        rclone_destination,
        rclone_files.abs(),
        expected_count,
        tx,
        config,
    )
    .await?;

    result_handler.await??;

    Ok(())
}

const TIMEOUT: time::Duration = time::Duration::from_millis(5);

fn write_rclone_files_fsck_clone(
    local: &impl Config,
    rclone_files: PathBuf,
) -> (JoinHandle<Result<(), InternalError>>, mpsc::Sender<String>) {
    let channel_buffer_size = local.buffer_size(BufferType::FsckRcloneFilesWriterChannelSize);
    let (tx, rx) = mpsc::channel::<String>(channel_buffer_size);

    let writer_buffer_size = local.buffer_size(BufferType::FsckRcloneFilesStreamChunkSize);
    let writing_task = tokio::spawn(async move {
        let file = File::create(rclone_files)
            .await
            .map_err(InternalError::IO)?;
        let mut writer = BufWriter::new(file);

        let mut chunked_stream = futures::StreamExt::boxed(
            ReceiverStream::new(rx)
                .track("write_rclone_files_fsck_clone::rx")
                .chunks_timeout(writer_buffer_size, TIMEOUT),
        );
        while let Some(chunk) = chunked_stream.next().await {
            let data: String = chunk.into_iter().fold(String::new(), |mut acc, path| {
                acc.push_str(&(path + "\n"));
                acc
            });

            writer
                .write_all(data.as_bytes())
                .await
                .map_err(InternalError::IO)?;
        }
        writer.flush().await.map_err(InternalError::IO)?;
        Ok::<(), InternalError>(())
    });

    (writing_task, tx)
}

enum RCloneResult {
    Success(String),
    Failure(String),
}

async fn execute_rclone(
    temp_path: &Path,
    source: impl RCloneTarget,
    destination: impl RCloneTarget,
    rclone_files_path: &Path,
    expected_count: u64,
    listener: mpsc::UnboundedSender<RCloneResult>,
    config: RCloneConfig,
) -> Result<u64, InternalError> {
    let count = Arc::new(AtomicU64::new(0));
    let failed_count = Arc::new(AtomicU64::new(0));
    let start_time = tokio::time::Instant::now();
    let mut obs = Observer::without_id("rclone");
    obs.observe_length(log::Level::Trace, expected_count);
    let mut detail_obs = Observer::without_id("rclone:detail");

    if expected_count == 0 {
        Observer::without_id("rclone").observe_termination(log::Level::Info, "no files to verify");
        return Ok(0);
    }

    let count_clone = Arc::clone(&count);
    let failed_count_clone = Arc::clone(&failed_count);
    let mut files: HashMap<String, Observer<BaseObservable>> = HashMap::new();
    let callback = move |event: RcloneEvent| {
        match event {
            RcloneEvent::Ok(object) => {
                let new_count = count_clone.fetch_add(1, Ordering::Relaxed) + 1;
                obs.observe_position(log::Level::Trace, new_count);
                listener.send(RCloneResult::Success(object)).unwrap();
            }
            RcloneEvent::Fail(object) => {
                let new_count = count_clone.fetch_add(1, Ordering::Relaxed) + 1;
                obs.observe_position(log::Level::Trace, new_count);
                failed_count_clone.fetch_add(1, Ordering::Relaxed);
                listener.send(RCloneResult::Failure(object)).unwrap();
            }
            RcloneEvent::UnknownMessage(msg) => {
                detail_obs.observe_state(log::Level::Debug, msg);
            }
            RcloneEvent::Error(err) => {
                obs.observe_state(log::Level::Error, err);
            }
            RcloneEvent::UnchangedSkipping(_) => {}
            RcloneEvent::Copied(_) => {}
            RcloneEvent::Stats(RcloneStats {
                bytes,
                eta,
                total_bytes,
                transferring: checking,
                ..
            }) => {
                let mut seen_files = HashSet::new();

                for item in checking {
                    seen_files.insert(item.name.clone());
                    let f = files.entry(item.name.clone()).or_insert_with(|| {
                        Observer::with_auto_termination(
                            BaseObservable::with_id("rclone:file", item.name.clone()),
                            log::Level::Debug,
                            BaseObservation::TerminalState("done".into()),
                        )
                    });
                    f.observe_state(
                        log::Level::Debug,
                        format!(
                            "{}/{}",
                            units::human_readable_size(item.bytes),
                            units::human_readable_size(item.size),
                        ),
                    );
                }

                files.retain(|key, _| seen_files.contains(key));

                let msg = format!(
                    "{}/{} ETA: {}",
                    units::human_readable_size(bytes),
                    units::human_readable_size(total_bytes),
                    match eta {
                        None => "-".into(),
                        Some(eta) => format!("{}s", eta),
                    }
                );
                obs.observe_state(log::Level::Debug, msg);
            }
            RcloneEvent::UnknownDebugMessage(_) => {}
        };
    };

    let result = run_rclone(
        Operation::Check,
        temp_path,
        rclone_files_path,
        source,
        destination,
        config,
        callback,
    )
    .await;

    let final_failed_count = failed_count.load(Ordering::Relaxed);
    let final_count = count.load(Ordering::Relaxed);
    let duration = start_time.elapsed();

    let msg = if final_failed_count > 0 {
        format!("detected {final_failed_count} altered files in {duration:.2?}")
    } else {
        format!("verified {final_count} files in {duration:.2?}")
    };

    if let Err(InternalError::RClone(code)) = result {
        if code != 1 {
            result?;
        } else {
            Observer::without_id("rclone").observe_state(
                log::Level::Debug,
                "rclone with exit code 1 - the errors have been passed through already",
            );
        }
    } else {
        result?;
    }
    Observer::without_id("rclone").observe_termination(log::Level::Info, msg);

    Ok(final_count)
}
