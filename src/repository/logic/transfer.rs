use crate::db::models::TransferItem;
use crate::repository::traits::{BlobReceiver, BlobSender, Local, Metadata};
use crate::utils::internal_error::InternalError;
use crate::utils::pipe::TryForwardIntoExt;
use futures::StreamExt;
use rand::Rng;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;

fn execute_rclone(rclone_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    todo!() // TODO
}

pub async fn transfer(
    local: &(impl Local + Send + Sync + Clone + 'static),
    source: &(impl Metadata + BlobSender + Send + Sync + Clone + 'static),
    target: &(impl BlobReceiver + Send + Sync + Clone + 'static),
    connection: (),
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = rand::thread_rng();
    let transfer_id: u32 = rng.gen();

    let transfer_path = local.transfer_path(transfer_id);
    let rclone_files = transfer_path.join("rclone-files.txt");

    let source_repo_id = source.repo_id().await?;
    let stream = target
        .create_transfer_request(transfer_id, source_repo_id)
        .await;

    {
        let (tx, mut rx) = mpsc::channel::<TransferItem>(100);

        // Spawn a task to handle writing to the file
        let rclone_files_clone = rclone_files.clone();
        let writing_task = tokio::spawn(async move {
            let file = File::create(rclone_files_clone).await?;
            let mut writer = BufWriter::new(file);

            while let Some(item) = rx.recv().await {
                writer
                    .write_all(format!("{}\n", item.path).as_bytes())
                    .await
                    .map_err(InternalError::IO)?;
            }
            writer.flush().await.map_err(InternalError::IO)?;
            Ok::<(), InternalError>(())
        });

        stream
            .then(move |t| {
                let tx = tx.clone();
                async move {
                    if let Ok(ref item) = t {
                        tx.send(item.clone()).await.map_err(|_| {
                            InternalError::Stream("failed to send to writer".into())
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
    execute_rclone(rclone_files)?;

    target.finalise_transfer(transfer_id).await?;

    Ok(())
}
