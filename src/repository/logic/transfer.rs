use crate::db::models::TransferItem;
use crate::repository::connection::EstablishedConnection;
use crate::repository::traits::{BlobReceiver, BlobSender, BufferType, Config, Local, Metadata};
use crate::utils::errors::InternalError;
use crate::utils::pipe::TryForwardIntoExt;
use crate::utils::rclone::{
    run_rclone_operation, LocalConfig, RcloneEvent, RcloneStats, RcloneTarget,
};
use futures::StreamExt;
use log::debug;
use rand::Rng;
use std::path::PathBuf;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

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
    let (tx, mut rx) =
        mpsc::channel::<TransferItem>(local.buffer_size(BufferType::TransferRcloneFilesWriter));

    let writing_task = tokio::spawn(async move {
        let file = match File::create(rclone_files).await {
            Ok(file) => file,
            Err(err) => {
                debug!("error creating file: {}", err);
                return Err(err.into());
            }
        };
        let mut writer = BufWriter::new(file);

        while let Some(TransferItem { path, .. }) = rx.recv().await {
            writer
                .write_all(format!("{path}\n").as_bytes())
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
    rclone_path: PathBuf,
) -> Result<(), InternalError> {
    let local_target = RcloneTarget::Local(LocalConfig {
        path: connection.local.root().abs(),
    });
    let remote_target = connection.remote_rclone_target();

    let (source, destination) = match direction {
        Direction::Upload => (local_target, remote_target),
        Direction::Download => (remote_target, local_target),
    };

    let callback = |event: RcloneEvent| match event {
        RcloneEvent::Message(msg) => println!("[message] {}", msg),
        RcloneEvent::Error(err) => eprintln!("[error] {}", err),
        RcloneEvent::Stats(RcloneStats {
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
            ..
        }) => {
            println!(
                "[stats] {elapsed_time}/{eta:?}/{transfer_time} {bytes}B/{total_bytes}B {transfers}/{total_transfers} {speed} B/s (E:{errors} R: {retry_error} F: {fatal_error})",
            );
        }
    };

    let from_or_to = match direction {
        Direction::Upload => "to",
        Direction::Download => "to",
    };
    debug!("copying files {from_or_to} {0}", connection.name);
    run_rclone_operation(
        &connection.local.transfer_path(transfer_id).abs(),
        &rclone_path,
        source,
        destination,
        callback,
    )
    .await?;

    Ok(())
}

pub async fn transfer(
    local: &(impl Metadata + Local + Send + Sync + Clone + Config + 'static),
    source: &(impl Metadata + BlobSender + Send + Sync + Clone + 'static),
    destination: &(impl BlobReceiver + Send + Sync + Clone + 'static),
    connection: EstablishedConnection,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = rand::rng();
    let transfer_id: u32 = rng.random();
    debug!("current transfer_id={transfer_id}");

    let transfer_path = local.transfer_path(transfer_id);
    fs::create_dir_all(&transfer_path).await?;
    let rclone_files = transfer_path.join("rclone.files");

    let local_repo_id = local.repo_id().await?;
    let source_repo_id = source.repo_id().await?;
    let direction = match local_repo_id == source_repo_id {
        true => Direction::Upload,
        false => Direction::Download,
    };
    debug!("local={local_repo_id} source={source_repo_id} -> {direction:?}");

    let stream = destination
        .create_transfer_request(transfer_id, source_repo_id)
        .await;

    let rclone_files_clone = rclone_files.clone();
    {
        let (writing_task, tx) = write_rclone_files_clone(local, rclone_files_clone.abs().clone());
        stream
            .then(move |t| {
                let tx = tx.clone();
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
    }
    debug!(
        "rclone.files has been written to {}",
        rclone_files.display()
    );

    execute_rclone(connection, transfer_id, direction, rclone_files.abs()).await?;
    debug!("rclone has copied the files");

    destination.finalise_transfer(transfer_id).await?;

    // TODO: cleanup DB + staging folder - local + remote

    Ok(())
}
