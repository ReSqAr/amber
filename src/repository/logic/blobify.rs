use crate::db::models::{InsertBlob, InsertFile};
use crate::repository::local_repository::LocalRepository;
use crate::repository::traits::{Local, Metadata};
use crate::utils::sha256;
use async_lock::Mutex;
use filetime::{set_file_times, FileTime};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::{fs, task};
use uuid::Uuid;

pub(crate) type BlobLockMap = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;

pub(crate) async fn blobify(
    local_repository: &LocalRepository,
    path: String,
    dry_run: bool,
    blob_locks: BlobLockMap,
) -> Result<(Option<InsertFile>, Option<InsertBlob>), Box<dyn std::error::Error>> {
    let file_path = local_repository.root().join(&path);
    let (blob_id, blob_size) = sha256::compute_sha256_and_size(&file_path).await?;
    let blob_path = local_repository.blob_path(blob_id.clone());
    let staging_path =
        local_repository
            .staging_path()
            .join(format!("{}.{}", blob_id, Uuid::now_v7()));

    if dry_run {
        debug!(
            "dry-run: would compute hash and size for {}",
            file_path.display()
        );
        debug!("dry-run: would check existence of {}", blob_path.display());
        if !fs::metadata(&blob_path)
            .await
            .ok()
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            debug!(
                "dry-run: would hard link {} to {}",
                file_path.display(),
                blob_path.display()
            );
        } else {
            debug!(
                "dry-run: would hard link {} to {}",
                blob_path.display(),
                staging_path.display()
            );
            debug!(
                "dry-run: would copy attributes from {} to {}",
                file_path.display(),
                staging_path.display()
            );
            debug!(
                "dry-run: would atomically swap {} with {}",
                staging_path.display(),
                file_path.display()
            );
        }
        return Ok((None, None));
    }

    {
        let mut locks = blob_locks.lock().await;
        let blob_lock = locks
            .entry(blob_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        drop(locks);

        let _lock_guard = blob_lock.lock().await;

        if !fs::metadata(&blob_path)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            // Scenario 1: blob_path does not exist
            fs::hard_link(&file_path, &blob_path).await?;
            debug!(
                "hard linked {} to {}",
                file_path.display(),
                blob_path.display()
            );

            let valid_from = chrono::Utc::now();
            let blob = Some(InsertBlob {
                repo_id: local_repository.repo_id().await?,
                blob_id: blob_id.clone(),
                blob_size,
                has_blob: true,
                valid_from,
            });

            let file = Some(InsertFile {
                path,
                blob_id: Some(blob_id.clone()),
                valid_from,
            });

            return Ok((file, blob));
        }
        // Lock is released here as _lock_guard goes out of scope
    }

    // Scenario 2: blob_path already exists
    fs::hard_link(&blob_path, &staging_path).await?;
    debug!(
        "hard linked {} to {}",
        blob_path.display(),
        staging_path.display()
    );

    let metadata = fs::metadata(&file_path).await?;
    fs::set_permissions(&staging_path, metadata.permissions()).await?;

    let accessed_time = FileTime::from_last_access_time(&metadata);
    let modified_time = FileTime::from_last_modification_time(&metadata);
    let staging_path_clone = staging_path.clone();
    task::spawn_blocking(move || set_file_times(&staging_path_clone, accessed_time, modified_time))
        .await??;

    // atomically swap file_path and staging_path
    fs::rename(&staging_path, &file_path).await?;
    debug!(
        "atomically swapped {} with {}",
        staging_path.display(),
        file_path.display()
    );

    let valid_from = chrono::Utc::now();
    Ok((
        Some(InsertFile {
            path,
            blob_id: Some(blob_id.clone()),
            valid_from,
        }),
        Some(InsertBlob {
            repo_id: local_repository.repo_id().await?,
            blob_id: blob_id.clone(),
            blob_size,
            has_blob: true,
            valid_from,
        }),
    ))
}
