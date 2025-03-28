use crate::connection::EstablishedConnection;
use crate::db::models::{AvailableBlob, ObservedBlob};
use crate::flightdeck::base::BaseObserver;
use crate::flightdeck::base::{BaseObservable, BaseObservation};
use crate::flightdeck::observer::Observer;
use crate::flightdeck::stream::Trackable;
use crate::logic::files;
use crate::repository::traits::{
    Adder, Availability, BufferType, Config, Local, Metadata, RcloneTargetPath,
};
use crate::utils::errors::InternalError;
use crate::utils::rclone::{Operation, RCloneTarget, RcloneEvent, RcloneStats, run_rclone};
use crate::utils::units;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

pub(crate) async fn fsck_remote(
    local: &(impl Config + Local + Adder + Sync + Send + Clone + 'static),
    remote: &(impl Metadata + Availability + RcloneTargetPath),
    connection: &EstablishedConnection,
) -> Result<(), InternalError> {
    let staging_id: u32 = rand::rng().random();
    let fsck_path = local.staging_id_path(staging_id);
    let fsck_files_path = local.rclone_target_path(staging_id);
    let start_time = tokio::time::Instant::now();
    fs::create_dir_all(&fsck_path).await?;
    let rclone_files = fsck_path.join("rclone.files");
    let rclone_source =
        connection.local_rclone_target(fsck_files_path.abs().to_string_lossy().into());
    let rclone_destination = connection.remote_rclone_target(remote.rclone_path(staging_id).await?);

    let expected_count = {
        let (writing_task, tx) =
            write_rclone_files_fsck_clone(local, rclone_files.clone().abs().clone());

        let stream = remote.available();
        let local_clone = local.clone();
        let fsck_files_path_clone = fsck_files_path.clone();
        let stream = StreamExt::map(stream, move |blob: Result<AvailableBlob, InternalError>| {
            let local = local_clone.clone();
            let fsck_files_path = fsck_files_path_clone.clone();
            let tx = tx.clone();
            async move {
                let blob = blob?;
                let blob_path = local.blob_path(&blob.blob_id);
                let mut o = BaseObserver::with_id("fsck:file:materialise", blob.blob_id.clone());

                if fs::metadata(&blob_path)
                    .await
                    .map(|m| m.is_file())
                    .unwrap_or(false)
                {
                    if let Some(path) = blob.path {
                        files::create_hard_link(&blob_path, &fsck_files_path.join(&path)).await?;
                        tx.send(path).await?;
                        o.observe_termination(log::Level::Debug, "materialised");
                    } else {
                        o.observe_termination(log::Level::Debug, "skipped");
                    }
                } else {
                    o.observe_termination(log::Level::Debug, "skipped");
                }

                Ok::<(), InternalError>(())
            }
        });

        let mut stream = futures::StreamExt::buffer_unordered(
            stream,
            local.buffer_size(BufferType::FsckMaterialiseBufferParallelism),
        );

        let mut count = 0u64;
        let mut obs = BaseObserver::without_id("fsck:files:materialise");
        while let Some(result) = stream.next().await {
            count += 1;
            obs.observe_position(log::Level::Trace, count);
            result?;
        }

        drop(stream);
        writing_task.await??;

        let duration = start_time.elapsed();
        let msg = format!("materialised {} blobs for fsck in {duration:.2?}", count);
        obs.observe_termination(log::Level::Info, msg);

        count
    };

    let (tx, rx) = mpsc::unbounded_channel::<RCloneResult>();
    let local_clone = local.clone();
    let repo_id = remote.current().await?.id;
    let result_handler = tokio::spawn(async move {
        let stream = UnboundedReceiverStream::new(rx).track("fsck_remote::rx");
        let stream = stream.map(|r: RCloneResult| match r {
            RCloneResult::Success(object) => {
                BaseObserver::with_id("rclone:check", object.clone())
                    .observe_termination(log::Level::Debug, "OK");
                ObservedBlob {
                    repo_id: repo_id.clone(),
                    has_blob: true,
                    path: object,
                    valid_from: chrono::Utc::now(),
                }
            }
            RCloneResult::Failure(object) => {
                BaseObserver::with_id("rclone:check", object.clone())
                    .observe_termination(log::Level::Error, "altered");
                ObservedBlob {
                    repo_id: repo_id.clone(),
                    has_blob: false,
                    path: object,
                    valid_from: chrono::Utc::now(),
                }
            }
        });
        local_clone.observe_blobs(stream).await?;

        Ok::<(), InternalError>(())
    });

    execute_rclone(
        &fsck_path.abs().clone(),
        rclone_source,
        rclone_destination,
        rclone_files.abs(),
        expected_count,
        tx,
    )
    .await?;

    result_handler.await??;

    Ok(())
}

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

        let mut chunked_stream = futures::StreamExt::ready_chunks(
            ReceiverStream::new(rx).track("write_rclone_files_fsck_clone::rx"),
            writer_buffer_size,
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
