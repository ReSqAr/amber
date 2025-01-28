use crate::db::models::{InsertBlob, InsertFile};
use crate::repository::traits::{Local, Metadata};
use crate::utils::fs::are_hardlinked;
use crate::utils::path::RepoPath;
use crate::utils::sha256;
use async_lock::Mutex;
use filetime::{set_file_times, FileTime};
use log::debug;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::{fs, task};
use uuid::Uuid;

pub(crate) type BlobLockMap = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;

async fn create_hard_link_or_dry_run(
    source: &RepoPath,
    destination: &RepoPath,
    dry_run: bool,
) -> Result<(), Box<dyn Error>> {
    if dry_run {
        debug!(
            "dry-run: would hard link {} to {}",
            source.display(),
            destination.display()
        );
    } else {
        fs::hard_link(source, destination).await?;
        debug!(
            "hard linked {} to {}",
            source.display(),
            destination.display()
        );
    }
    Ok(())
}
async fn force_hard_link_with_rename_or_dry_run(
    local: &impl Local,
    source: &RepoPath,
    destination: &RepoPath,
    blob_id: &str,
    dry_run: bool,
) -> Result<(), Box<dyn Error>> {
    if dry_run {
        debug!(
            "dry-run: would hard link {} to staging path",
            source.display()
        );
        debug!(
            "dry-run: would copy attributes from {} to staging path",
            source.display()
        );
        debug!(
            "dry-run: would atomically swap staging path with {}",
            destination.display()
        );
    } else {
        // Compute staging_path using blob_id and a unique UUID
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

        // Atomically move
        fs::rename(&staging_path, destination).await?;
        debug!(
            "atomically moved {} to {}",
            staging_path.display(),
            destination.display()
        );
    }
    Ok(())
}

pub(crate) async fn blobify(
    local: &(impl Local + Metadata),
    path: String,
    dry_run: bool,
    blob_locks: BlobLockMap,
) -> Result<(Option<InsertFile>, Option<InsertBlob>), Box<dyn Error>> {
    let file_path = local.root().join(&path);
    let (blob_id, blob_size) = sha256::compute_sha256_and_size(&file_path).await?;
    let blob_path = local.blob_path(blob_id.clone());

    let (file, blob) = if !dry_run {
        let valid_from = chrono::Utc::now();
        (
            Some(InsertFile {
                path,
                blob_id: Some(blob_id.clone()),
                valid_from,
            }),
            Some(InsertBlob {
                repo_id: local.repo_id().await?,
                blob_id: blob_id.clone(),
                blob_size,
                has_blob: true,
                valid_from,
            }),
        )
    } else {
        (None, None)
    };

    // Acquire lock for the current blob_id
    {
        let mut locks = blob_locks.lock().await;
        let blob_lock = locks
            .entry(blob_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        drop(locks); // Release the lock map

        let _lock_guard = blob_lock.lock().await; // Acquire the blob-specific lock

        // Scenario 1: blob_path does not exist
        if !fs::metadata(&blob_path)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            create_hard_link_or_dry_run(&file_path, &blob_path, dry_run).await?;
            return Ok((file, blob));
        }
        // Lock is released here as `_lock_guard` goes out of scope
    }

    // Scenario 2 & 3: blob_path exists
    if !are_hardlinked(&blob_path, &file_path).await? {
        force_hard_link_with_rename_or_dry_run(local, &blob_path, &file_path, &blob_id, dry_run)
            .await?;
    } else {
        debug!(
            "{} and {} are already hard linked. No action needed.",
            blob_path.display(),
            file_path.display()
        );
    }

    Ok((file, blob))
}
