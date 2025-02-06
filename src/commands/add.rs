use crate::db::models::{VirtualFile, VirtualFileState};
use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObservable, BaseObservation, StateTransformer, Style,
    TerminationAction,
};
use crate::flightdeck::observer::Observer;
use crate::flightdeck::progress_manager::LayoutItemBuilderNode;
use crate::repository::local::LocalRepository;
use crate::repository::logic::blobify::BlobLockMap;
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

pub async fn add(maybe_root: Option<PathBuf>, dry_run: bool) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root).await?;
    let root_path = local_repository.root().abs().clone();
    let log_path = local_repository.log_path().abs().into();

    let wrapped = async {
        add_files(local_repository, dry_run).await?;
        Ok::<(), InternalError>(())
    };

    flightdeck::flightdeck(
        wrapped,
        root_builders(&root_path),
        log_path,
        log::LevelFilter::Info,
    )
    .await
}

fn root_builders(root_path: &Path) -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let root = root_path.display().to_string() + "/";

    let file = BaseLayoutBuilderBuilder::default()
        .type_key("sha")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdStateFn(Box::new(move |done, id, _| {
            let id = id.unwrap_or("<missing>".into());
            let path = id.strip_prefix(root.as_str()).unwrap_or(root.as_str());
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
        .build()
        .expect("build should work")
        .boxed();

    let overall = BaseLayoutBuilderBuilder::default()
        .type_key("adder")
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("done".into()),
                false => msg.unwrap_or("adding".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} {pos}".into(),
            done: "{prefix}✓ {msg} ({elapsed})".into(),
        })
        .build()
        .expect("build should work")
        .boxed();

    [LayoutItemBuilderNode::from(overall).add_child(file)]
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
    dry_run: bool,
) -> Result<(), InternalError> {
    let mut adder_obs = Observer::new(BaseObservable::without_id("adder".into()));

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

    fs::create_dir_all(&repository.staging_path()).await?;
    let (state_handle, stream) = state::state(repository.clone(), WalkerConfig::default()).await?;

    let stream = futures::TryStreamExt::try_filter(stream, |file_result| {
        let state = file_result.state.clone();
        async move { state.unwrap_or(VirtualFileState::NeedsCheck) == VirtualFileState::New }
    });

    {
        // scope to isolate the effects of the below wild channel cloning
        let blob_locks: BlobLockMap = Arc::new(async_lock::Mutex::new(HashMap::new()));
        let file_tx_clone = file_tx.clone();
        let blob_tx_clone = blob_tx.clone();
        let stream = tokio_stream::StreamExt::map(
            stream,
            |file_result: Result<VirtualFile, InternalError>| {
                let file_tx_clone = file_tx_clone.clone();
                let blob_tx_clone = blob_tx_clone.clone();
                let local_repository_clone = repository.clone();
                let blob_locks_clone = blob_locks.clone();
                async move {
                    let path = local_repository_clone.root().join(file_result?.path);
                    let (insert_file, insert_blob) =
                        blobify::blobify(&local_repository_clone, &path, dry_run, blob_locks_clone)
                            .await?;
                    if let Some(file) = insert_file {
                        file_tx_clone.send(file).await?;
                    }
                    if let Some(blob) = insert_blob {
                        blob_tx_clone.send(blob).await?;
                    }
                    Ok::<RepoPath, InternalError>(path)
                }
            },
        );

        // Allow multiple blobify operations to run concurrently
        let stream = futures::StreamExt::buffer_unordered(
            stream,
            repository.buffer_size(BufferType::AddFilesBlobifyFutureFileBuffer),
        );

        let mut count = 0;
        pin_mut!(stream);
        while let Some(maybe_path) = tokio_stream::StreamExt::next(&mut stream).await {
            match maybe_path {
                Ok(_path) => {
                    count += 1;
                    adder_obs.observe(log::Level::Trace, BaseObservation::Position(count));
                }
                Err(e) => {
                    println!("error: {e}");
                }
            }
        }

        let msg = if count > 0 {
            format!("added {count} files")
        } else {
            "no files added".into()
        };
        adder_obs.observe(log::Level::Info, BaseObservation::TerminalState(msg));

        state_handle.await??;
    }

    drop(file_tx);
    drop(blob_tx);
    db_file_handle.await??;
    db_blob_handle.await??;

    Ok(())
}
