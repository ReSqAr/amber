use futures::{pin_mut, TryStreamExt};
use std::collections::HashMap;
use tokio::fs;

use crate::db::models::VirtualFileState;
use crate::repository::local_repository::LocalRepository;
use crate::repository::logic::blobify::BlobLockMap;
use crate::repository::logic::state::StateConfig;
use crate::repository::logic::{blobify, state};
use crate::repository::traits::{Adder, Local};
use anyhow::Context;
use async_lock::Mutex;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

pub async fn add_file(dry_run: bool) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    let (state_handle, stream) =
        state::state(local_repository.clone(), StateConfig::default()).await?;

    let (file_tx, file_rx) = mpsc::channel(100);
    let (blob_tx, blob_rx) = mpsc::channel(100);

    let local_repository_clone = local_repository.clone();
    let db_file_handle = tokio::spawn(async move {
        local_repository_clone
            .add_files(ReceiverStream::new(file_rx))
            .await
    });
    let local_repository_clone = local_repository.clone();
    let db_blob_handle = tokio::spawn(async move {
        local_repository_clone
            .add_blobs(ReceiverStream::new(blob_rx))
            .await
    });

    fs::create_dir_all(&local_repository.staging_path())
        .await
        .context("unable to create staging directory")?;

    let blob_locks: BlobLockMap = Arc::new(Mutex::new(HashMap::new()));
    let stream = stream
        .try_filter(|file_result| {
            let state = file_result.state.clone();
            async move { state.unwrap_or(VirtualFileState::NeedsCheck) == VirtualFileState::New }
        })
        .then(|file_result| {
            let tx_file_clone = file_tx.clone();
            let tx_blob_clone = blob_tx.clone();
            let local_repository_clone = local_repository.clone();
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
                    tx_file_clone.send(file).await?;
                }
                if let Some(blob) = insert_blob {
                    tx_blob_clone.send(blob).await?;
                }
                Ok::<String, Box<dyn std::error::Error>>(path)
            }
        });

    pin_mut!(stream);

    let mut count = 0;
    while let Some(maybe_path) = stream.next().await {
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
    drop(blob_tx);
    drop(file_tx);

    println!("added: {}", count);

    state_handle.await??;
    db_file_handle.await??;
    db_blob_handle.await??;

    Ok(())
}
