use crate::db::models::{InsertBlob, InsertFile, InsertMaterialisation};
use crate::flightdeck;
use crate::flightdeck::base::BaseObserver;
use crate::logic::blobify::{BlobLockMap, Blobify};
use crate::logic::state::{VirtualFile, VirtualFileState};
use crate::logic::{blobify, state};
use crate::repository::traits::{Adder, BufferType, Config, Local, Metadata, VirtualFilesystem};
use crate::utils::errors::InternalError;
use crate::utils::path::RepoPath;
use crate::utils::walker::WalkerConfig;
use dashmap::DashMap;
use futures::pin_mut;
use std::sync::Arc;
use tokio::fs;

pub(crate) async fn add_files(
    repository: impl Metadata
    + Local
    + Adder
    + VirtualFilesystem
    + Config
    + Clone
    + Send
    + Sync
    + 'static,
    skip_deduplication: bool,
) -> Result<(), InternalError> {
    let start_time = tokio::time::Instant::now();
    let mut scanner_obs = BaseObserver::without_id("scanner");
    let mut adder_obs = BaseObserver::without_id("adder");

    let (file_tx, file_rx) = flightdeck::tracked::mpsc_channel(
        "add_files::file",
        repository.buffer_size(BufferType::AddFilesDBAddFilesChannelSize),
    );
    let db_file_handle = {
        let local_repository = repository.clone();
        tokio::spawn(async move {
            local_repository
                .add_files(file_rx)
                .await
                .inspect_err(|e| log::error!("add_files: add_files task failed: {e}"))
        })
    };
    let (blob_tx, blob_rx) = flightdeck::tracked::mpsc_channel(
        "add_files::blob",
        repository.buffer_size(BufferType::AddFilesDBAddBlobsChannelSize),
    );
    let db_blob_handle = {
        let local_repository = repository.clone();
        tokio::spawn(async move {
            local_repository
                .add_blobs(blob_rx)
                .await
                .inspect_err(|e| log::error!("add_files: add_blobs task failed: {e}"))
        })
    };
    let (mat_tx, mat_rx) = flightdeck::tracked::mpsc_channel(
        "add_files::mat",
        repository.buffer_size(BufferType::AddFilesDBAddMaterialisationsChannelSize),
    );
    let db_mat_handle = {
        let local_repository = repository.clone();
        tokio::spawn(async move {
            local_repository
                .add_materialisation(mat_rx)
                .await
                .inspect_err(|e| log::error!("add_files: add_materialisation task failed: {e}"))
        })
    };

    fs::create_dir_all(&repository.staging_path()).await?;
    let (state_handle, stream) = state::state(repository.clone(), WalkerConfig::default()).await?;

    let mut scan_count = 0;
    let stream = futures::TryStreamExt::try_filter(stream, |file_result| {
        scan_count += 1;
        scanner_obs.observe_position(log::Level::Trace, scan_count);

        let state = file_result.state.clone();
        async move {
            match state {
                VirtualFileState::New => true,
                VirtualFileState::Missing { .. } => false,
                VirtualFileState::Ok { .. } => false,
                VirtualFileState::OkMaterialisationMissing { .. } => true,
                VirtualFileState::Altered { .. } => false,
                VirtualFileState::Outdated { .. } => false,
            }
        }
    });

    let mut count = 0;
    {
        // scope to isolate the effects of the below wild channel cloning
        let blob_locks: BlobLockMap = Arc::new(DashMap::new());
        let file_tx = file_tx.clone();
        let blob_tx = blob_tx.clone();
        let mat_tx = mat_tx.clone();
        let stream = tokio_stream::StreamExt::map(
            stream,
            |file_result: Result<VirtualFile, InternalError>| {
                let file_tx = file_tx.clone();
                let blob_tx = blob_tx.clone();
                let mat_tx = mat_tx.clone();
                let local_repository_clone = repository.clone();
                let blob_locks_clone = blob_locks.clone();
                async move {
                    let path = local_repository_clone.root().join(file_result?.path);
                    let Blobify { blob_id, blob_size } = blobify::blobify(
                        &local_repository_clone,
                        &path,
                        skip_deduplication,
                        blob_locks_clone,
                    )
                    .await?;

                    let valid_from = chrono::Utc::now();
                    let file = InsertFile {
                        path: path.rel().to_string_lossy().to_string(),
                        blob_id: Some(blob_id.clone()),
                        valid_from,
                    };
                    let blob = InsertBlob {
                        repo_id: local_repository_clone.current().await?.id,
                        blob_id: blob_id.clone(),
                        blob_size: blob_size as i64,
                        has_blob: true,
                        path: None,
                        valid_from,
                    };
                    let mat = InsertMaterialisation {
                        path: path.rel().to_string_lossy().to_string(),
                        blob_id: Some(blob_id.clone()),
                        valid_from,
                    };

                    file_tx.send(file).await?;
                    blob_tx.send(blob).await?;
                    mat_tx.send(mat).await?;
                    Ok::<RepoPath, InternalError>(path)
                }
            },
        );

        // allow multiple blobify operations to run concurrently
        let stream = futures::StreamExt::buffer_unordered(
            stream,
            repository.buffer_size(BufferType::AddFilesBlobifyFutureFileParallelism),
        );

        pin_mut!(stream);
        while let Some(maybe_path) = tokio_stream::StreamExt::next(&mut stream).await {
            let path = maybe_path?;
            BaseObserver::with_id("add", path.rel().display().to_string())
                .observe_termination(log::Level::Info, "added");

            count += 1;
            adder_obs.observe_position(log::Level::Trace, count);
        }

        state_handle.await??;
    }

    drop(file_tx);
    drop(blob_tx);
    drop(mat_tx);
    db_file_handle.await??;
    db_blob_handle.await??;
    db_mat_handle.await??;

    let duration = start_time.elapsed();
    let msg = if count > 0 {
        format!("added {count} files in {duration:.2?}")
    } else {
        "no files added".into()
    };
    adder_obs.observe_termination(log::Level::Info, msg);
    let msg = if scan_count > 0 {
        format!("scanned {scan_count} files in {duration:.2?}")
    } else {
        "no files scanned".into()
    };
    scanner_obs.observe_termination(log::Level::Debug, msg);

    Ok(())
}
