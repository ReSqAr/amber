use crate::db::models::BlobID;
use crate::logic::files;
use crate::repository::traits::{Local, Metadata};
use crate::utils::blake3;
use crate::utils::errors::InternalError;
use crate::utils::path::RepoPath;
use async_lock::Mutex;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::fs;

pub(crate) type BlobLockMap = Arc<DashMap<BlobID, Arc<Mutex<()>>>>;

pub(crate) struct Blobify {
    pub(crate) blob_id: BlobID,
    pub(crate) blob_size: u64,
}

pub(crate) async fn blobify(
    local: &(impl Local + Metadata),
    path: &RepoPath,
    blob_locks: BlobLockMap,
) -> Result<Blobify, InternalError> {
    let blake3::HashWithSize {
        hash: blob_id,
        size: blob_size,
    } = blake3::compute_blake3_and_size(path).await?;
    let result = Blobify {
        blob_id: blob_id.clone(),
        blob_size,
    };
    let blob_path = local.blob_path(&blob_id);

    // make path read only
    let mut permissions = fs::metadata(path).await?.permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions).await?;
    }

    // acquire lock for the current blob_id
    {
        let blob_lock = blob_locks
            .entry(blob_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        let _lock_guard = blob_lock.lock().await; // Acquire the blob-specific lock

        // scenario 1: blob_path does not exist (content is not known yet)
        if !fs::metadata(&blob_path)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            files::create_link(path, &blob_path, local.capability()).await?;
            return Ok(result);
        }
        // lock is released here as `_lock_guard` goes out of scope
    }

    Ok(result)
}
