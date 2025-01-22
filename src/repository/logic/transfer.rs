use crate::repository::traits::{BlobReceiver, BlobSender, Local, Metadata};
use crate::utils::app_error::AppError;
use crate::utils::pipe::TryForwardIntoExt;
use futures::StreamExt;
use std::path::PathBuf;

fn execute_rclone(rclone_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    todo!() // TODO
}

pub async fn transfer(
    local: &(impl Local + Send + Sync + Clone + 'static),
    source: &(impl Metadata + BlobSender + Send + Sync + Clone + 'static),
    target: &(impl BlobReceiver + Send + Sync + Clone + 'static),
    connection: (),
) -> Result<(), Box<dyn std::error::Error>> {
    let transfer_id = 42u32; // random

    let staging_path = local.staging_path();
    let rclone_path = staging_path.join(format!("rclone_{}.txt", transfer_id));

    let source_repo_id = source.repo_id().await?;
    let stream = target
        .create_transfer_request(transfer_id, source_repo_id)
        .await;

    // TODO: write to local rclone file: rclone_path
    stream
        .try_forward_into::<_, _, _, _, AppError>(|s| {
            source.prepare_transfer(s.inspect(|t| println!("{:?}", t)))
        })
        .await?;

    execute_rclone(rclone_path)?;

    target.finalise_transfer(transfer_id).await?;

    Ok(())
}
