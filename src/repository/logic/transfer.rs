use crate::db::models::TransferItem;
use crate::flightdeck::base::{BaseObservable, BaseObservation, BaseObserver};
use crate::flightdeck::observer::Observer;
use crate::repository::connection::EstablishedConnection;
use crate::repository::traits::{BlobReceiver, BlobSender, BufferType, Config, Local, Metadata};
use crate::utils::errors::InternalError;
use crate::utils::pipe::TryForwardIntoExt;
use crate::utils::rclone::{
    run_rclone_operation, LocalConfig, RcloneEvent, RcloneStats, RcloneTarget,
};
use futures::StreamExt;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug)]
enum Direction {
    Upload,
    Download,
}

fn write_rclone_files_clone(
    local: &(impl Local + Config + Sized),
    rclone_files: PathBuf,
) -> (
    JoinHandle<Result<(), InternalError>>,
    mpsc::Sender<TransferItem>,
) {
    let channel_buffer_size = local.buffer_size(BufferType::TransferRcloneFilesWriter);
    let (tx, rx) = mpsc::channel::<TransferItem>(channel_buffer_size);

    let writer_buffer_size = local.buffer_size(BufferType::TransferRcloneFilesStream);
    let writing_task = tokio::spawn(async move {
        let file = File::create(rclone_files)
            .await
            .map_err(InternalError::IO)?;
        let mut writer = BufWriter::new(file);

        let mut chunked_stream = ReceiverStream::new(rx).ready_chunks(writer_buffer_size);
        while let Some(chunk) = chunked_stream.next().await {
            let data: String = chunk.into_iter().fold(String::new(), |mut acc, item| {
                acc.push_str(&(item.path + "\n"));
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
    connection: EstablishedConnection,
    transfer_id: u32,
    direction: Direction,
    rclone_path: &Path,
    expected_count: u64,
) -> Result<u64, InternalError> {
    let count = Arc::new(AtomicU64::new(0));
    let start_time = tokio::time::Instant::now();
    let mut obs = Observer::without_id("rclone");
    obs.observe_length(log::Level::Trace, expected_count);
    let mut detail_obs = Observer::without_id("rclone:detail");

    let local_target = RcloneTarget::Local(LocalConfig {
        path: connection.local.root().abs().clone(),
    });
    let remote_target = connection.remote_rclone_target();

    let (source, destination) = match direction {
        Direction::Upload => (local_target, remote_target),
        Direction::Download => (remote_target, local_target),
    };

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
            }
            RcloneEvent::Stats(RcloneStats {
                // TODO
                bytes,
                elapsed_time,
                errors,
                eta,
                fatal_error,
                retry_error,
                speed,
                total_bytes,
                total_transfers,
                transfer_time,
                transfers,
                transferring,
                ..
            }) => {
                let mut seen_files = HashSet::new();

                for transfer in transferring {
                    seen_files.insert(transfer.name.clone());
                    let f = files.entry(transfer.name.clone()).or_insert_with(|| {
                        Observer::with_auto_termination(
                            BaseObservable::with_id("rclone:file", transfer.name.clone()),
                            log::Level::Info,
                            BaseObservation::TerminalState("done".into()),
                        )
                    });
                    f.observe_state(
                        log::Level::Debug,
                        format!("{}/{} {}", transfer.bytes, transfer.size, transfer.eta),
                    );
                }

                files.retain(|key, _| seen_files.contains(key));

                let msg = format!(
                    "{elapsed_time}/{eta:?}/{transfer_time} {bytes}B/{total_bytes}B {transfers}/{total_transfers} {speed} B/s (E:{errors} R: {retry_error} F: {fatal_error})",
                );
                obs.observe_state(log::Level::Debug, msg);
            }
        };
    };

    run_rclone_operation(
        connection.local.transfer_path(transfer_id).abs(),
        rclone_path,
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

pub async fn transfer(
    local: &(impl Metadata + Local + Send + Sync + Clone + Config + 'static),
    source: &(impl Metadata + BlobSender + Send + Sync + Clone + 'static),
    destination: &(impl Metadata + BlobReceiver + Send + Sync + Clone + 'static),
    connection: EstablishedConnection,
) -> Result<u64, InternalError> {
    let mut rng = rand::rng();
    let transfer_id: u32 = rng.random();

    let mut transfer_obs = BaseObserver::without_id("transfer");

    let transfer_path = local.transfer_path(transfer_id);
    fs::create_dir_all(&transfer_path).await?;
    fs::create_dir_all(&transfer_path).await?;
    let rclone_files = transfer_path.join("rclone.files");

    let local_repo_id = local.repo_id().await?;
    let source_repo_id = source.repo_id().await?;
    let destination_repo_id = destination.repo_id().await?;
    let direction = match local_repo_id == source_repo_id {
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
            ("source".into(), source_repo_id.clone()),
            ("destination".into(), destination_repo_id.clone()),
        ],
    );

    let start_time = tokio::time::Instant::now();
    let stream = destination
        .create_transfer_request(transfer_id, source_repo_id)
        .await;

    transfer_obs.observe_state(log::Level::Debug, "preparing");
    let expected_count = {
        let mut prep_count = 0;
        let mut prep_obs = Observer::with_auto_termination(
            BaseObservable::without_id("transfer:preparation"),
            log::Level::Debug,
            BaseObservation::TerminalState("done".into()),
        );
        let rclone_files_clone = rclone_files.clone();
        let (writing_task, tx) = write_rclone_files_clone(local, rclone_files_clone.abs().clone());
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
        "selected no files transfer".into()
    };
    transfer_obs.observe_state(log::Level::Info, msg);

    transfer_obs.observe_state(log::Level::Debug, "copying");
    execute_rclone(
        connection,
        transfer_id,
        direction,
        rclone_files.abs(),
        expected_count,
    )
    .await?;

    let start_time = tokio::time::Instant::now();
    transfer_obs.observe_state(log::Level::Debug, "verifying");
    let count = destination.finalise_transfer(transfer_id).await?;
    let msg = if count > 0 {
        let duration = start_time.elapsed();
        format!("verified {count} blobs in {duration:.2?}")
    } else {
        "no files verified".into()
    };
    transfer_obs.observe_state(log::Level::Info, msg);

    transfer_obs.observe_termination(log::Level::Debug, "done");

    // TODO: cleanup staging folder - local + remote

    Ok(count)
}

pub(crate) async fn cleanup_staging(local: &impl Local) -> Result<(), InternalError> {
    let staging_path = local.staging_path();
    match fs::remove_dir_all(&staging_path).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    };
    Ok(())
}
