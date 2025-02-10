use crate::db::models::{InsertBlob, InsertFile};
use crate::repository::traits::{Local, Metadata};
use crate::utils::errors::InternalError;
use crate::utils::fs::are_hardlinked;
use crate::utils::path::RepoPath;
use crate::utils::sha256;
use async_lock::Mutex;
use filetime::{set_file_times, FileTime};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::{fs, task};
use uuid::Uuid;

pub(crate) type BlobLockMap = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;

async fn create_hard_link(source: &RepoPath, destination: &RepoPath) -> Result<(), InternalError> {
    if let Some(parent) = destination.abs().parent() {
        fs::create_dir_all(parent).await?;
    }

    fs::hard_link(source, destination).await?;
    debug!(
        "hard linked {} to {}",
        source.display(),
        destination.display()
    );

    Ok(())
}

async fn forced_atomic_hard_link(
    local: &impl Local,
    source: &RepoPath,
    destination: &RepoPath,
    blob_id: &str,
) -> Result<(), InternalError> {
    // compute staging_path using blob_id and a unique UUID
    // we use the staging file to set the permissions/file times
    // before atomically overwriting the user's file
    let staging_path = local
        .staging_path()
        .join(format!("{}.{}", blob_id, Uuid::now_v7()));

    fs::hard_link(source, &staging_path).await?;
    debug!(
        "hard linked {} to {}",
        source.display(),
        staging_path.display()
    );

    let metadata = fs::metadata(source).await?;
    fs::set_permissions(&staging_path, metadata.permissions()).await?;
    debug!("set permissions for {}", staging_path.display());

    let accessed_time = FileTime::from_last_access_time(&metadata);
    let modified_time = FileTime::from_last_modification_time(&metadata);
    let staging_path_clone = staging_path.clone();
    task::spawn_blocking(move || set_file_times(&staging_path_clone, accessed_time, modified_time))
        .await??;
    debug!("set file times for {}", staging_path.display());

    // atomically over-write the file - we trust upstream that the
    // file we overwrite has the same content as the existing file
    fs::rename(&staging_path, destination).await?;
    debug!(
        "atomically moved {} to {}",
        staging_path.display(),
        destination.display()
    );

    Ok(())
}

pub(crate) async fn blobify(
    local: &(impl Local + Metadata),
    path: &RepoPath,
    deduplicate: bool,
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
            create_hard_link(path, &blob_path).await?;
            return Ok((file, blob));
        }
        // lock is released here as `_lock_guard` goes out of scope
    }

    if !are_hardlinked(&blob_path, &path).await? {
        // scenario 2: blob_path exists and is a carbon copy of $path, but they are not hard-linked
        if deduplicate {
            forced_atomic_hard_link(local, &blob_path, path, &blob_id).await?;
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
