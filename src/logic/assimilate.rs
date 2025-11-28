use crate::db::models::{BlobID, InsertBlob, RepoID};
use crate::flightdeck::observer::Observer;
use crate::logic::files;
use crate::repository::traits::{Adder, BufferType, Config, Local, Metadata};
use crate::utils::blake3;
use crate::utils::errors::{AppError, InternalError};
use crate::utils::path::RepoPath;
use crate::utils::pipe::TryForwardIntoExt;
use async_lock::Mutex;
use dashmap::DashMap;
use futures::{StreamExt, TryStreamExt};
use futures_core::stream::BoxStream;
use log::debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs;

pub(crate) type BlobLockMap = Arc<DashMap<BlobID, Arc<Mutex<()>>>>;

#[derive(Debug)]
pub struct Item {
    pub path: RepoPath,
    pub expected_blob_id: Option<BlobID>,
}

#[allow(clippy::collapsible_if)]
async fn assimilate_blob(
    local: &impl Local,
    repo_id: RepoID,
    item: Item,
    blob_locks: BlobLockMap,
) -> Result<InsertBlob, InternalError> {
    let file_path = item.path;
    let blake3::HashWithSize {
        hash: blob_id,
        size: blob_size,
    } = blake3::compute_blake3_and_size(&file_path).await?;
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

    let blob_path = local.blob_path(&blob_id);
    {
        let blob_lock = blob_locks
            .entry(blob_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        let _lock_guard = blob_lock.lock().await; // acquire the blob-specific lock

        if !fs::metadata(&blob_path)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            files::assimilate(&file_path, &blob_path).await?;
        } else {
            debug!("blob {} already exists, skipping", blob_id.0);
        }
        // lock is released here as `_lock_guard` goes out of scope
    }

    Ok::<InsertBlob, InternalError>(InsertBlob {
        repo_id,
        blob_id,
        blob_size,
        has_blob: true,
        path: None,
        valid_from: chrono::Utc::now(),
    })
}

pub(crate) async fn assimilate<'a>(
    local: &'a (impl Local + Metadata + Adder + Send + Sync + Config),
    stream: BoxStream<'a, Item>,
) -> Result<u64, InternalError> {
    let start_time = tokio::time::Instant::now();
    let mut obs = Observer::without_id("assimilate");

    let blob_locks: BlobLockMap = Arc::new(DashMap::new());
    let meta = local.current().await?;
    let counter = Arc::new(AtomicU64::new(0));

    let counter_clone = counter.clone();
    let mut value = obs.clone();
    let count = stream
        .map(move |i| assimilate_blob(local, meta.id.clone(), i, blob_locks.clone()))
        .buffer_unordered(local.buffer_size(BufferType::AssimilateParallelism))
        .inspect_ok(move |_| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            value.observe_position(log::Level::Trace, counter.load(Ordering::Relaxed));
        })
        .try_forward_into::<_, _, _, _, InternalError>(|s| async move {
            local.add_blobs(s.boxed()).await
        })
        .await?;

    let msg = if count > 0 {
        let duration = start_time.elapsed();
        format!("assimilated {count} blobs in {duration:.2?}")
    } else {
        "no files to be assimilated".into()
    };
    obs.observe_termination(log::Level::Info, msg);

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::local::{LocalRepository, LocalRepositoryConfig};
    use tempfile::tempdir;
    use tokio::fs;
    use tokio::io::AsyncWriteExt;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_assimilate_single_file() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let repo_path = dir.path().join("repo");
        fs::create_dir_all(&repo_path).await?;

        let local = LocalRepository::create(
            LocalRepositoryConfig {
                maybe_root: Some(repo_path.clone()),
                app_folder: ".amb".into(),
                preferred_capability: None,
            },
            "test_repo".into(),
        )
        .await?;

        let file_path = repo_path.join("hello.txt");
        let mut file = fs::File::create(&file_path).await?;
        file.write_all(b"Hello world!").await?;

        let items = futures::stream::iter([Item {
            path: local.root().join("hello.txt"),
            expected_blob_id: None,
        }])
        .boxed();

        // run assimilate
        let count = assimilate(&local, items).await?;
        assert_eq!(count, 1);

        let blob_file = repo_path
            .join(".amb/blobs/79/3c/10bc0b28c378330d39edace7260af9da81d603b8ffede2706a21eda893f4");
        let buf = fs::read(&blob_file).await?;
        assert_eq!(buf, b"Hello world!");

        local.close().await?;
        Ok(())
    }
}
