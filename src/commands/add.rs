use crate::db::models::{InsertBlob, InsertFile, InsertMaterialisation};
use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::repository::local::LocalRepository;
use crate::repository::logic::blobify::{BlobLockMap, Blobify};
use crate::repository::logic::state::{VirtualFile, VirtualFileState};
use crate::repository::logic::{blobify, state};
use crate::repository::traits::{Adder, BufferType, Config, Local, Metadata, VirtualFilesystem};
use crate::utils::errors::InternalError;
use crate::utils::path::RepoPath;
use crate::utils::walker::WalkerConfig;
use futures::pin_mut;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub async fn add(
    maybe_root: Option<PathBuf>,
    skip_deduplication: bool,
    verbose: bool,
) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root).await?;
    let root_path = local_repository.root().abs().clone();
    let log_path = local_repository.log_path().abs().clone();

    let wrapped = async {
        add_files(local_repository, skip_deduplication).await?;
        Ok::<(), InternalError>(())
    };

    let terminal = match verbose {
        true => Some(log::LevelFilter::Debug),
        false => None,
    };
    flightdeck::flightdeck(wrapped, root_builders(&root_path), log_path, None, terminal).await
}

fn root_builders(root_path: &Path) -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let root = root_path.display().to_string() + "/";

    let file = BaseLayoutBuilderBuilder::default()
        .type_key("sha")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdFn(Box::new(move |done, id| {
            let id = id.unwrap_or("<missing>".into());
            let path = id.strip_prefix(root.as_str()).unwrap_or(id.as_str());
            match done {
                true => format!("hashed {}", path),
                false => format!("hashing {}", path),
            }
        })))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} {decimal_bytes}/{decimal_total_bytes}"
                .into(),
            done: "{prefix}✓ {msg} {decimal_bytes}".into(),
        })
        .infallible_build()
        .boxed();

    let adder = BaseLayoutBuilderBuilder::default()
        .type_key("adder")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("done".into()),
                false => msg.unwrap_or("adding".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({pos})".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    [LayoutItemBuilderNode::from(adder).add_child(file)]
}

pub async fn add_files(
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
    let mut adder_obs = BaseObserver::without_id("adder");

    let (file_tx, file_rx) = mpsc::channel(repository.buffer_size(BufferType::AddFilesDBAddFiles));
    let db_file_handle = {
        let local_repository = repository.clone();
        tokio::spawn(async move {
            local_repository
                .add_files(ReceiverStream::new(file_rx))
                .await
        })
    };
    let (blob_tx, blob_rx) = mpsc::channel(repository.buffer_size(BufferType::AddFilesDBAddBlobs));
    let db_blob_handle = {
        let local_repository = repository.clone();
        tokio::spawn(async move {
            local_repository
                .add_blobs(ReceiverStream::new(blob_rx))
                .await
        })
    };
    let (mat_tx, mat_rx) =
        mpsc::channel(repository.buffer_size(BufferType::AddFilesDBAddMaterialisations));
    let db_mat_handle = {
        let local_repository = repository.clone();
        tokio::spawn(async move {
            local_repository
                .add_materialisation(ReceiverStream::new(mat_rx))
                .await
        })
    };

    fs::create_dir_all(&repository.staging_path()).await?;
    let (state_handle, stream) = state::state(repository.clone(), WalkerConfig::default()).await?;

    let stream = futures::TryStreamExt::try_filter(stream, |file_result| {
        let state = file_result.state.clone();
        async move { state == VirtualFileState::New }
    });

    let mut count = 0;
    {
        // scope to isolate the effects of the below wild channel cloning
        let blob_locks: BlobLockMap = Arc::new(async_lock::Mutex::new(HashMap::new()));
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
                        repo_id: local_repository_clone.repo_id().await?,
                        blob_id: blob_id.clone(),
                        blob_size: blob_size as i64,
                        has_blob: true,
                        valid_from,
                    };
                    let mat = InsertMaterialisation {
                        path: path.rel().to_string_lossy().to_string(),
                        blob_id: blob_id.clone(),
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
            repository.buffer_size(BufferType::AddFilesBlobifyFutureFileBuffer),
        );

        pin_mut!(stream);
        while let Some(maybe_path) = tokio_stream::StreamExt::next(&mut stream).await {
            match maybe_path {
                Ok(path) => {
                    BaseObserver::with_id("add", path.rel().display().to_string())
                        .observe_termination(log::Level::Info, "added");

                    count += 1;
                    adder_obs.observe_position(log::Level::Trace, count);
                }
                Err(e) => {
                    println!("error: {e}");
                }
            }
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

    Ok(())
}
