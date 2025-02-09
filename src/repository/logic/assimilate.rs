use crate::db::models::InsertBlob;
use crate::repository::traits::{Adder, BufferType, Config, Local, Metadata};
use crate::utils::errors::{AppError, InternalError};
use crate::utils::pipe::TryForwardIntoExt;
use crate::utils::sha256;
use async_lock::Mutex;
use futures::{Stream, StreamExt};
use log::warn;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::fs;

pub(crate) type BlobLockMap = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;

#[derive(Debug)]
pub struct Item {
    pub path: String,
    pub expected_blob_id: Option<String>,
}

async fn assimilate_blob(
    local: &impl Local,
    repo_id: String,
    item: Item,
    blob_locks: BlobLockMap,
) -> Result<InsertBlob, InternalError> {
    let file_path = local.root().join(item.path);
    let (blob_id, blob_size) = sha256::compute_sha256_and_size(&file_path).await?;
    if let Some(expected_blob_id) = item.expected_blob_id {
        if expected_blob_id != blob_id {
            return Err(AppError::UnexpectedBlobId {
                path: file_path.display().to_string(),
                expected: expected_blob_id,
                actual: blob_id,
            }
            .into());
        }
    }

    let blob_path = local.blob_path(blob_id.clone());
    {
        let mut locks = blob_locks.lock().await;
        let blob_lock = locks
            .entry(blob_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        drop(locks); // Release the lock map

        let _lock_guard = blob_lock.lock().await; // acquire the blob-specific lock
        if !fs::metadata(&blob_path)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            if let Some(parent) = blob_path.abs().parent() {
                fs::create_dir_all(parent).await?;
            }

            fs::rename(file_path, blob_path).await?;
        } else {
            warn!("blob {blob_id} already exists, skipping");
        }
        // lock is released here as `_lock_guard` goes out of scope
    }

    Ok::<InsertBlob, InternalError>(InsertBlob {
        repo_id,
        blob_id,
        blob_size: blob_size as i64,
        has_blob: true,
        valid_from: chrono::Utc::now(),
    })
}

pub(crate) async fn assimilate<S>(
    local: &(impl Local + Metadata + Adder + Send + Sync + Config),
    stream: S,
) -> Result<u64, InternalError>
where
    S: Stream<Item = Item> + Unpin + Send + 'static,
{
    let blob_locks: BlobLockMap = Arc::new(Mutex::new(HashMap::new()));
    let repo_id = local.repo_id().await?;
    stream
        .map(move |i| assimilate_blob(local, repo_id.clone(), i, blob_locks.clone()))
        .buffer_unordered(local.buffer_size(BufferType::Assimilate))
        .try_forward_into::<_, _, _, _, InternalError>(|s| async { local.add_blobs(s).await })
        .await
}
