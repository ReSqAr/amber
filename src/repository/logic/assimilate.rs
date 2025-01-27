use crate::db::models::InsertBlob;
use crate::repository::traits::{Adder, Local, Metadata};
use crate::utils::errors::{AppError, InternalError};
use crate::utils::pipe::TryForwardIntoExt;
use crate::utils::sha256;
use futures::{Stream, StreamExt};
use tokio::fs;

#[derive(Debug)]
pub struct Item {
    pub path: String,
    pub expected_blob_id: Option<String>,
}

async fn assimilate_blob(
    local: &impl Local,
    repo_id_clone: String,
    transfer_id_clone: u32,
    item: Item,
) -> Result<InsertBlob, InternalError> {
    let file_path = local.transfer_path(transfer_id_clone).join(item.path);
    let (blob_id, blob_size) = sha256::compute_sha256_and_size(&file_path).await?;
    if let Some(expected_blob_id) = item.expected_blob_id {
        if expected_blob_id != blob_id {
            return Err(AppError::UnexpectedBlobId(format!(
                "path: {} expected: {} actual: {}",
                file_path.display(),
                expected_blob_id,
                blob_id
            ))
            .into());
        }
    }

    let target_path = local.blob_path(blob_id.clone());
    fs::rename(file_path, target_path).await?; // TODO: check if exists (noop) + lock for parallel access

    Ok::<InsertBlob, InternalError>(InsertBlob {
        repo_id: repo_id_clone,
        blob_id,
        blob_size,
        has_blob: true,
        valid_from: chrono::Utc::now(),
    })
}

pub(crate) async fn assimilate<S>(
    local: &(impl Local + Metadata + Adder + Send + Sync),
    transfer_id: u32,
    stream: S,
) -> Result<(), InternalError>
where
    S: Stream<Item = Item> + Unpin + Send + 'static,
{
    let repo_id = local.repo_id().await?;
    stream
        .map(move |i| assimilate_blob(local, repo_id.clone(), transfer_id, i))
        .buffer_unordered(100) // TODO: constant via config
        .try_forward_into::<_, _, _, _, InternalError>(|s| async { local.add_blobs(s).await })
        .await
}
