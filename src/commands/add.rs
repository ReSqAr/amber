use futures::stream;
use std::path::PathBuf;
use tokio::{fs, io};

use crate::db::models::{InsertBlob, InsertFile};
use crate::repository::local_repository::LocalRepository;
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use crate::repository::traits::{Adder, Local, Metadata};

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

pub async fn add_file(path: String) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    let current_path = fs::canonicalize(".").await?;
    let blob_path = local_repository.blob_path();

    let file_path = current_path.join(&path);
    let (blob_id, blob_size) = compute_sha256_and_size(&file_path).await?;
    let object_path = blob_path.join(&blob_id);

    if !fs::metadata(&object_path)
        .await
        .map(|m| m.is_file())
        .unwrap_or(false)
    {
        fs::hard_link(&file_path, &object_path).await?;
    };

    let valid_from = chrono::Utc::now();
    let f = InsertFile {
        path,
        blob_id: Some(blob_id.clone()),
        valid_from,
    };
    let sf = stream::iter(vec![f]);
    local_repository.add_files(sf).await?;
    let b = InsertBlob {
        repo_id: local_repository.repo_id().await?,
        blob_id,
        blob_size,
        has_blob: true,
        valid_from,
    };
    let sb = stream::iter(vec![b]);
    local_repository.add_blobs(sb).await?;

    Ok(())
}
