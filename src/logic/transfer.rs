use crate::connection::EstablishedConnection;
use crate::db::models::CopiedTransferItem;
use crate::flightdeck::base::{BaseObservable, BaseObservation, BaseObserver};
use crate::flightdeck::observer::Observer;
use crate::repository::traits::{
    BufferType, Config, Local, Metadata, RcloneTargetPath, Receiver, Sender, TransferItem,
};
use crate::utils::errors::{AppError, InternalError};
use crate::utils::path::RepoPath;
use crate::utils::pipe::TryForwardIntoExt;
use crate::utils::rclone::{Operation, RCloneTarget, RcloneEvent, RcloneStats, run_rclone};
use crate::utils::units;
use futures::StreamExt;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

#[derive(Debug)]
enum Direction {
    Upload,
    Download,
}

fn write_rclone_files_clone<T: TransferItem>(
    local: &impl Config,
    rclone_files: PathBuf,
) -> (JoinHandle<Result<(), InternalError>>, mpsc::Sender<T>) {
    let channel_buffer_size = local.buffer_size(BufferType::TransferRcloneFilesWriterChunkSize);
    let (tx, rx) = mpsc::channel::<T>(channel_buffer_size);

    let writer_buffer_size = local.buffer_size(BufferType::TransferRcloneFilesStreamChunkSize);
    let writing_task = tokio::spawn(async move {
        let file = File::create(rclone_files)
            .await
            .map_err(InternalError::IO)?;
        let mut writer = BufWriter::new(file);

        let mut chunked_stream = ReceiverStream::new(rx).ready_chunks(writer_buffer_size);
        while let Some(chunk) = chunked_stream.next().await {
            let data: String = chunk.into_iter().fold(String::new(), |mut acc, item| {
                acc.push_str(&(item.path() + "\n"));
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

async fn execute_rclone(
    temp_path: &Path,
    source: impl RCloneTarget,
    destination: impl RCloneTarget,
    rclone_files_path: &Path,
    expected_count: u64,
    tx: UnboundedSender<String>,
) -> Result<u64, InternalError> {
    let count = Arc::new(AtomicU64::new(0));
    let start_time = tokio::time::Instant::now();
    let mut obs = Observer::without_id("rclone");
    obs.observe_length(log::Level::Trace, expected_count);
    let mut detail_obs = Observer::without_id("rclone:detail");

    if expected_count == 0 {
        Observer::without_id("rclone")
            .observe_termination(log::Level::Info, "no blobs to transfer");
        return Ok(0);
    }

    let count_clone = Arc::clone(&count);
    let mut files: HashMap<String, Observer<BaseObservable>> = HashMap::new();
    let callback = move |event: RcloneEvent| {
        match event {
            RcloneEvent::UnknownMessage(msg) => {
                detail_obs.observe_state(log::Level::Debug, msg);
            }
            RcloneEvent::Error(err) => {
                obs.observe_state(log::Level::Error, err);
            }
            RcloneEvent::Copied(name) => {
                let mut f = files
                    .remove(&name)
                    .unwrap_or_else(|| Observer::with_id("rclone:file", name.clone()));
                f.observe_termination(log::Level::Debug, "done");

                let new_count = count_clone.fetch_add(1, Ordering::Relaxed) + 1;
                obs.observe_position(log::Level::Trace, new_count);

                if let Err(e) = tx.send(name) {
                    log::error!("error while sending completion message: {e}")
                }
            }
            RcloneEvent::Stats(RcloneStats {
                bytes,
                eta,
                total_bytes,
                transferring,
                speed,
            }) => {
                let mut seen_files = HashSet::new();

                for transfer in transferring {
                    seen_files.insert(transfer.name.clone());
                    let f = files.entry(transfer.name.clone()).or_insert_with(|| {
                        Observer::with_auto_termination(
                            BaseObservable::with_id("rclone:file", transfer.name.clone()),
                            log::Level::Debug,
                            BaseObservation::TerminalState("done".into()),
                        )
                    });
                    f.observe_state(
                        log::Level::Debug,
                        format!(
                            "copying... ({}/{})",
                            units::human_readable_size(transfer.bytes),
                            units::human_readable_size(transfer.size),
                        ),
                    );
                }

                files.retain(|key, _| seen_files.contains(key));

                let msg = format!(
                    "ETA: {} speed: {}/s ({}/{})",
                    match eta {
                        None => "-".into(),
                        Some(eta) => format!("{}s", eta),
                    },
                    units::human_readable_size(speed.unwrap_or(0f64) as u64),
                    units::human_readable_size(bytes),
                    units::human_readable_size(total_bytes),
                );
                obs.observe_state(log::Level::Debug, msg);
            }
            RcloneEvent::Ok(_) => {}
            RcloneEvent::Fail(_) => {}
            RcloneEvent::UnknownDebugMessage(_) => {}
        };
    };

    run_rclone(
        Operation::Copy,
        temp_path,
        rclone_files_path,
        source,
        destination,
        callback,
    )
    .await?;

    let final_count = count.load(Ordering::Relaxed);
    let duration = start_time.elapsed();
    let msg = format!("copied {} blobs in {duration:.2?}", final_count);
    Observer::without_id("rclone").observe_termination(log::Level::Info, msg);
    Ok(final_count)
}

pub async fn transfer<T: TransferItem>(
    local: &(impl Metadata + Local + Send + Sync + Clone + Config + 'static),
    source: &(impl Metadata + Sender<T> + RcloneTargetPath + Send + Sync + Clone + 'static),
    destination: &(impl Metadata + Receiver<T> + RcloneTargetPath + Send + Sync + Clone + 'static),
    connection: EstablishedConnection,
    paths: Vec<RepoPath>,
) -> Result<u64, InternalError> {
    let transfer_id: u32 = rand::rng().random();

    let mut transfer_obs = BaseObserver::without_id("transfer");

    let transfer_path = local.staging_id_path(transfer_id);
    fs::create_dir_all(&transfer_path).await?;
    let rclone_files = transfer_path.join("rclone.files");

    let paths = paths
        .iter()
        .map(|p| p.rel().to_string_lossy().to_string())
        .collect();

    let local_meta = local.current().await?;
    let source_meta = source.current().await?;
    let destination_meta = destination.current().await?;
    let direction = match local_meta.id == source_meta.id {
        true => Direction::Upload,
        false => Direction::Download,
    };
    transfer_obs.observe_state_ext(
        log::Level::Debug,
        match direction {
            Direction::Upload => "upload",
            Direction::Download => "download",
        },
        [
            ("source".into(), source_meta.id.clone()),
            ("destination".into(), destination_meta.id.clone()),
        ],
    );

    let source_rclone_path = source.rclone_path(transfer_id).await?;
    let destination_rclone_path = destination.rclone_path(transfer_id).await?;
    let (rclone_source, rclone_destination) = match direction {
        Direction::Upload => (
            connection.local_rclone_target(source_rclone_path),
            connection.remote_rclone_target(destination_rclone_path),
        ),
        Direction::Download => (
            connection.remote_rclone_target(source_rclone_path),
            connection.local_rclone_target(destination_rclone_path),
        ),
    };

    let start_time = tokio::time::Instant::now();
    let stream = destination
        .create_transfer_request(transfer_id, source_meta.id, paths)
        .await;

    transfer_obs.observe_state(log::Level::Debug, "preparing");
    let expected_count = {
        let mut prep_count = 0;
        let mut prep_obs = Observer::with_auto_termination(
            BaseObservable::without_id("transfer:preparation"),
            log::Level::Debug,
            BaseObservation::TerminalState("done".into()),
        );
        let (writing_task, tx) =
            write_rclone_files_clone::<T>(local, rclone_files.clone().abs().clone());
        let count = stream
            .then(move |t| {
                let tx = tx.clone();
                prep_count += 1;
                prep_obs.observe_position(log::Level::Trace, prep_count);
                async move {
                    if let Ok(ref item) = t {
                        tx.send(item.clone()).await.map_err(|err| {
                            InternalError::Stream(format!(
                                "failed to send to rclone.files writer: {err}"
                            ))
                        })?;
                    }
                    t
                }
            })
            .boxed()
            .try_forward_into::<_, _, _, _, InternalError>(|s| source.prepare_transfer(s))
            .await?;

        writing_task.await??;

        count
    };
    let msg = if expected_count > 0 {
        let duration = start_time.elapsed();
        format!("selected {expected_count} blobs for transfer in {duration:.2?}")
    } else {
        "selected no files for transfer".into()
    };
    transfer_obs.observe_state(log::Level::Info, msg);

    let (tx, rx) = mpsc::unbounded_channel();
    let stream = UnboundedReceiverStream::new(rx);

    transfer_obs.observe_state(log::Level::Debug, "copying");
    let staging_path = local.staging_id_path(transfer_id);
    let rclone_handle = tokio::spawn(async move {
        execute_rclone(
            &staging_path.abs().clone(),
            rclone_source,
            rclone_destination,
            rclone_files.abs(),
            expected_count,
            tx,
        )
        .await
    });

    let stream = stream.map(move |path| CopiedTransferItem { path, transfer_id });
    let count = destination.finalise_transfer(stream).await?;

    rclone_handle.await??;

    transfer_obs.observe_termination(log::Level::Debug, "done");

    if count < expected_count {
        Err(AppError::IncompleteTransfer {
            count,
            expected_count,
        }
        .into())
    } else {
        Ok(count)
    }
}
