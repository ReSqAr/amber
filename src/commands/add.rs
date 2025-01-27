use crate::db::models::{VirtualFile, VirtualFileState};
use crate::repository::local::LocalRepository;
use crate::repository::logic::blobify::BlobLockMap;
use crate::repository::logic::state::{Error, StateConfig};
use crate::repository::logic::{blobify, state};
use crate::repository::traits::{Adder, BufferType, Config, Local, Metadata, VirtualFilesystem};
use anyhow::Context;
use async_lock::Mutex;
use futures::pin_mut;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub async fn add(dry_run: bool) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    add_files(local_repository, dry_run).await
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
) -> Result<(), Box<dyn std::error::Error>> {
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

    fs::create_dir_all(&repository.staging_path())
        .await
        .context("unable to create staging directory")?;
    let (state_handle, stream) = state::state(repository.clone(), StateConfig::default()).await?;

    let stream = futures::TryStreamExt::try_filter(stream, |file_result| {
        let state = file_result.state.clone();
        async move { state.unwrap_or(VirtualFileState::NeedsCheck) == VirtualFileState::New }
    });

    {
        // scope to isolate the effects of the below wild channel cloning
        let blob_locks: BlobLockMap = Arc::new(Mutex::new(HashMap::new()));
        let file_tx_clone = file_tx.clone();
        let blob_tx_clone = blob_tx.clone();
        let stream =
            tokio_stream::StreamExt::map(stream, |file_result: Result<VirtualFile, Error>| {
                let file_tx_clone = file_tx_clone.clone();
                let blob_tx_clone = blob_tx_clone.clone();
                let local_repository_clone = repository.clone();
                let blob_locks_clone = blob_locks.clone();
                async move {
                    let path = file_result?.path;
                    let (insert_file, insert_blob) = blobify::blobify(
                        &local_repository_clone,
                        path.clone(),
                        dry_run,
                        blob_locks_clone,
                    )
                    .await?;
                    if let Some(file) = insert_file {
                        file_tx_clone.send(file).await?;
                    }
                    if let Some(blob) = insert_blob {
                        blob_tx_clone.send(blob).await?;
                    }
                    Ok::<String, Box<dyn std::error::Error>>(path)
                }
            });

        // Allow multiple blobify operations to run concurrently
        let stream = futures::StreamExt::buffer_unordered(
            stream,
            repository.buffer_size(BufferType::AddFilesBlobifyFutureFileBuffer),
        );

        let mut count = 0;
        pin_mut!(stream);
        while let Some(maybe_path) = tokio_stream::StreamExt::next(&mut stream).await {
            match maybe_path {
                Ok(path) => {
                    println!("added {}", path);
                    count += 1;
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
        println!("added: {}", count);

        state_handle.await??;
    }

    drop(file_tx); // TODO: check that no messages get lost
    drop(blob_tx); // TODO: check that no messages get lost
    db_file_handle.await??;
    db_blob_handle.await??;

    Ok(())
}
