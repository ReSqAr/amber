use futures::{pin_mut, stream, TryStreamExt};
use log::debug;
use std::path::PathBuf;
use tokio::{fs, io};

use crate::db::models::{InsertBlob, InsertFile, VirtualFileState};
use crate::repository::local_repository::LocalRepository;
use crate::repository::logic::state;
use crate::repository::logic::state::StateConfig;
use crate::repository::traits::{Adder, Local, Metadata};
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

async fn compute_sha256_and_size(file_path: &PathBuf) -> io::Result<(String, i64)> {
    let mut file = File::open(file_path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 8192]; // 8KB buffer
    let mut size = 0i64;

    loop {
        let bytes_read = file.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
        size += bytes_read as i64;
    }

    let hash = hasher.finalize();
    Ok((format!("{:x}", hash), size))
}

pub async fn add_one_file(
    local_repository: &LocalRepository,
    path: String,
    dry_run: bool,
) -> Result<Option<InsertBlob>, Box<dyn std::error::Error>> {
    let file_path = local_repository.root().join(&path);
    let (blob_id, blob_size) = compute_sha256_and_size(&file_path).await?;
    let object_path = local_repository.blob_path(blob_id.clone());

    if !fs::metadata(&object_path)
        .await
        .map(|m| m.is_file())
        .unwrap_or(false)
    {
        if !dry_run {
            fs::hard_link(&file_path, &object_path).await?;
        } else {
            debug!(
                "dry-run: would hard link {} -> {}",
                object_path.display(),
                file_path.display()
            );
        }
    };

    let valid_from = chrono::Utc::now();
    let f = InsertFile {
        path,
        blob_id: Some(blob_id.clone()),
        valid_from,
    };
    let sf = stream::iter(vec![f]);
    local_repository.add_files(sf).await?;
    if !dry_run {
        Ok(Some(InsertBlob {
            repo_id: local_repository.repo_id().await?,
            blob_id,
            blob_size,
            has_blob: true,
            valid_from,
        }))
    } else {
        debug!("dry-run: would add db {}", blob_id);
        Ok(None)
    }
}

pub async fn add_file(dry_run: bool) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    let (state_handle, stream) = state::state(local_repository.clone(), StateConfig::default()).await?;

    let (tx, rx): (Sender<InsertBlob>, Receiver<InsertBlob>) = mpsc::channel(100);

    let local_repository_clone = local_repository.clone();
    let db_handle = tokio::spawn(async move {
        local_repository_clone
            .add_blobs(ReceiverStream::new(rx))
            .await
    });

    let stream = stream
        .try_filter(|file_result| {
            let state = file_result.state.clone();
            async move { state.unwrap_or(VirtualFileState::NeedsCheck) == VirtualFileState::New }
        })
        .then(|file_result| {
            let tx_clone = tx.clone();
            let local_repository_clone = local_repository.clone();
            async move {
                let path = file_result?.path;
                if let Some(blob) = add_one_file(&local_repository_clone, path.clone(), dry_run).await? {
                    tx_clone.send(blob).await?;
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
    drop(tx);

    println!("added: {}", count);

    state_handle.await??;
    db_handle.await??;
    Ok(())
}
