use crate::db::models::{InsertBlob, InsertFile};
use crate::repository::logic::files;
use crate::repository::traits::{Local, Metadata};
use crate::utils::errors::InternalError;
use crate::utils::fs::are_hardlinked;
use crate::utils::path::RepoPath;
use crate::utils::sha256;
use async_lock::Mutex;
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::fs;

pub(crate) type BlobLockMap = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;

pub(crate) async fn blobify(
    local: &(impl Local + Metadata),
    path: &RepoPath,
    skip_deduplication: bool,
    blob_locks: BlobLockMap,
) -> Result<(Option<InsertFile>, Option<InsertBlob>), InternalError> {
    let (blob_id, blob_size) = sha256::compute_sha256_and_size(&path).await?;
    let blob_path = local.blob_path(blob_id.clone());

    let valid_from = chrono::Utc::now();
    let file = Some(InsertFile {
        path: path.rel().to_string_lossy().to_string(),
        blob_id: Some(blob_id.clone()),
        valid_from,
    });
    let blob = Some(InsertBlob {
        repo_id: local.repo_id().await?,
        blob_id: blob_id.clone(),
        blob_size: blob_size as i64,
        has_blob: true,
        valid_from,
    });

    // make path read only
    let mut permissions = fs::metadata(path).await?.permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions).await?;
    }

    // acquire lock for the current blob_id
    {
        let mut locks = blob_locks.lock().await;
        let blob_lock = locks
            .entry(blob_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        drop(locks); // Release the lock map

        let _lock_guard = blob_lock.lock().await; // Acquire the blob-specific lock

        // scenario 1: blob_path does not exist (content is not known yet)
        if !fs::metadata(&blob_path)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            files::create_hard_link(path, &blob_path).await?;
            return Ok((file, blob));
        }
        // lock is released here as `_lock_guard` goes out of scope
    }

    if !are_hardlinked(&blob_path, &path).await? {
        // scenario 2: blob_path exists and is a carbon copy of $path, but they are not hard-linked
        if !skip_deduplication {
            files::forced_atomic_hard_link(local, &blob_path, path, &blob_id).await?;
        } else {
            debug!(
                "{} and {} are two different files with the same content. deduplication was not requested. no action needed.",
                blob_path.display(),
                path.display()
            );
        }
    } else {
        // scenario 3: blob_path exists and the files are hard-linked
        debug!(
            "{} and {} are already hard linked. no action needed.",
            blob_path.display(),
            path.display()
        );
    }

    Ok((file, blob))
}
