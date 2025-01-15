use std::path::PathBuf;
use log::debug;
use tokio::{fs, io};

use crate::db::db::DB;
use crate::db::establish_connection;
use crate::db::schema::run_migrations;
use crate::commands::errors::InvariableError;
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
    let current_path = fs::canonicalize(".").await?;
    let invariable_path = current_path.join(".inv");
    if !fs::metadata(&invariable_path)
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        return Err(InvariableError::NotInitialised().into());
    };

    let file_path = current_path.join(&path);
    if !fs::metadata(&file_path)
        .await
        .map(|m| m.is_file())
        .unwrap_or(false)
    {
        return Err(InvariableError::FileMissing(file_path.to_str().unwrap().to_string()).into());
    };

    let blob_path = invariable_path.join("blobs");
    fs::create_dir_all(blob_path.as_path()).await?;

    let object_id = compute_sha256(&file_path).await?;
    let object_path = blob_path.join(&object_id);

    if !fs::metadata(&object_path)
        .await
        .map(|m| m.is_file())
        .unwrap_or(false)
    {
        fs::hard_link(&file_path, &object_path).await?;
    };

    let db_path = invariable_path.join("db.sqlite");
    let pool = establish_connection(db_path.to_str().unwrap())
        .await
        .expect("failed to establish connection");
    run_migrations(&pool)
        .await
        .expect("failed to run migrations");

    let db = DB::new(pool.clone());
    let repo = db
        .get_or_create_current_repository()
        .await
        .expect("failed to create repo id");

    let file = db.add_file(&path, Some(&object_id), chrono::Utc::now())
        .await?;
    let blob = db.add_blob(&repo.repo_id, &object_id, chrono::Utc::now(), true)
        .await?;

    debug!("file: {:?}", file);
    debug!("blob: {:?}", blob);

    Ok(())
}
