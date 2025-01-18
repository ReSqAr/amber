use futures::stream;
use std::path::PathBuf;
use tokio::{fs, io};

use crate::db::models::{InputBlob, InputFile};
use crate::repository::local_repository::{Local, LocalRepository, Metadata};
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

async fn compute_sha256(file_path: &PathBuf) -> io::Result<String> {
    let mut file = File::open(file_path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 8192]; // 8KB buffer

    loop {
        let bytes_read = file.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    let hash = hasher.finalize();
    Ok(format!("{:x}", hash))
}

pub async fn add_file(path: String) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    let current_path = fs::canonicalize(".").await?;
    let blob_path = local_repository.blob_path();

    let file_path = current_path.join(&path);
    let object_id = compute_sha256(&file_path).await?;
    let object_path = blob_path.join(&object_id);

    if !fs::metadata(&object_path)
        .await
        .map(|m| m.is_file())
        .unwrap_or(false)
    {
        fs::hard_link(&file_path, &object_path).await?;
    };

    let valid_from = chrono::Utc::now();
    let f = InputFile {
        path,
        object_id: Some(object_id.clone()),
        valid_from,
    };
    let sf = stream::iter(vec![f]);
    local_repository.db.add_file(sf).await?;
    let b = InputBlob {
        repo_id: local_repository.repo_id(),
        object_id,
        has_blob: true,
        valid_from,
    };
    let sb = stream::iter(vec![b]);
    local_repository.db.add_blob(sb).await?;

    Ok(())
}
