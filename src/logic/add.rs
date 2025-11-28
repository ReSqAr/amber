use crate::db::models;
use crate::db::models::{
    FileCheck, InsertBlob, InsertFile, InsertFileBundle, InsertMaterialisation,
};
use crate::flightdeck;
use crate::flightdeck::base::BaseObserver;
use crate::flightdeck::tracer::Tracer;
use crate::logic::blobify::{BlobLockMap, Blobify};
use crate::logic::state::{VirtualFile, VirtualFileState};
use crate::logic::{blobify, state};
use crate::repository::traits::{Adder, BufferType, Config, Local, Metadata, VirtualFilesystem};
use crate::utils::errors::InternalError;
use crate::utils::path::RepoPath;
use crate::utils::walker::WalkerConfig;
use dashmap::DashMap;
use futures::{StreamExt, pin_mut, stream};
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
) -> Result<(), InternalError> {
    let start_time = tokio::time::Instant::now();
    let mut scanner_obs = BaseObserver::without_id("scanner");
    let mut adder_obs = BaseObserver::without_id("adder");

    let (bundle_tx, bundle_rx) = flightdeck::tracked::mpsc_channel(
        "add_files::bundle",
        repository.buffer_size(BufferType::AddFilesDBAddFilesChannelSize),
    );
    let bg_db_bundle = {
        let local_repository = repository.clone();
        tokio::spawn(async move {
            local_repository
                .add_file_bundles(bundle_rx.boxed())
                .await
                .inspect_err(|e| log::error!("add_files: add_file_bundles task failed: {e}"))
        })
    };

    let (check_tx, check_rx) = flightdeck::tracked::mpsc_channel(
        "add_files::check",
        repository.buffer_size(BufferType::AddFilesDBAddFilesChannelSize),
    );

    fs::create_dir_all(&repository.staging_path())
        .await
        .inspect_err(|e| log::error!("add_files: create_dir_all failed: {e}"))?;
    let (state_handle, stream, state_checks) =
        state::state_with_checks(repository.clone(), WalkerConfig::default()).await?;

    let local_repository = repository.clone();
    let bg_persist_check = tokio::spawn(async move {
        let fc = stream::select(state_checks, check_rx);
        local_repository.add_checked_events(fc.boxed()).await
    });

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
                VirtualFileState::OkBlobMissing { .. } => true,
                VirtualFileState::Altered { .. } => false,
                VirtualFileState::Outdated { .. } => false,
            }
        }
    });

    let repo_id = repository.current().await?.id;
    let mut count = 0;
    {
        let blob_locks: BlobLockMap = Arc::new(DashMap::new());
        let bundle_tx = bundle_tx.clone();
        let check_tx = check_tx.clone();
        let stream =
            tokio_stream::StreamExt::map(
                stream,
                |file_result: Result<VirtualFile, InternalError>| {
                    let repo_id = repo_id.clone();
                    let local_repository_clone = repository.clone();
                    let blob_locks_clone = blob_locks.clone();
                    let bundle_tx = bundle_tx.clone();
                    let check_tx = check_tx.clone();
                    async move {
                        let path = local_repository_clone.root().join(file_result?.path.0);
                        let Blobify { blob_id, blob_size } =
                            blobify::blobify(&local_repository_clone, &path, blob_locks_clone)
                                .await
                                .inspect_err(|e| log::error!("add_files: blobify failed: {e}"))?;

                        let now = chrono::Utc::now();
                        let p = models::Path(path.rel().to_string_lossy().to_string());
                        let file = InsertFile {
                            path: p.clone(),
                            blob_id: Some(blob_id.clone()),
                            valid_from: now,
                        };
                        let blob = InsertBlob {
                            repo_id,
                            blob_id: blob_id.clone(),
                            blob_size,
                            has_blob: true,
                            path: None,
                            valid_from: now,
                        };
                        let mat = InsertMaterialisation {
                            path: p.clone(),
                            blob_id: Some(blob_id.clone()),
                            valid_from: now,
                        };

                        let bundle = InsertFileBundle {
                            file,
                            blob,
                            materialisation: mat,
                        };

                        let check = FileCheck {
                            path: p,
                            check_dttm: now,
                            hash: blob_id.clone(),
                        };

                        bundle_tx.send(bundle).await.inspect_err(|e| {
                            log::error!("add_files: send on bundle_tx failed: {e}")
                        })?;
                        check_tx.send(check).await.inspect_err(|e| {
                            log::error!("add_files: send on check_tx failed: {e}")
                        })?;
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
            let path =
                maybe_path.inspect_err(|e| log::error!("add_files: main task failed: {e}"))?;
            BaseObserver::with_id("add", path.rel().display().to_string())
                .observe_termination(log::Level::Info, "added");

            count += 1;
            adder_obs.observe_position(log::Level::Trace, count);
        }

        let tracer = Tracer::new_on("add_files::state_handle::await");
        state_handle.await??;
        tracer.measure();
    }

    drop(bundle_tx);
    drop(check_tx);
    let tracer = Tracer::new_on("add_files::bg_db_bundle::await");
    bg_db_bundle.await??;
    tracer.measure();
    let tracer = Tracer::new_on("add_files::bg_persist_check::await");
    bg_persist_check.await??;
    tracer.measure();

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
