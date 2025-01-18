use crate::commands::errors::InvariableError;
use crate::db::db::DB;
use crate::db::establish_connection;
use crate::db::models::{BlobObjectId, CurrentRepository, FilePathWithObjectId};
use crate::db::schema::run_migrations;
use crate::transport::server::invariable::invariable_client::InvariableClient;
use crate::transport::server::invariable::{
    DownloadRequest, RepositoryIdRequest, RepositoryIdResponse,
};
use anyhow::{Context, Result};
use futures::StreamExt;
use log::{debug, info};
use std::path::PathBuf;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub async fn pull(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let current_path = fs::canonicalize(".").await?;
    let invariable_path = current_path.join(".inv");
    if !fs::metadata(&invariable_path)
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        return Err(InvariableError::NotInitialised().into());
    };

    let db_path = invariable_path.join("db.sqlite");
    let pool = establish_connection(db_path.to_str().unwrap())
        .await
        .context("failed to establish connection")?;
    run_migrations(&pool)
        .await
        .context("failed to run migrations")?;

    let db = DB::new(pool.clone());
    debug!("db connected");

    let CurrentRepository {
        repo_id: local_repo_id,
    } = db.get_or_create_current_repository().await?;
    debug!("local repo_id={}", local_repo_id);

    let addr = format!("http://127.0.0.1:{}", port);
    debug!("connecting to {}", &addr);
    let mut client = InvariableClient::connect(addr.clone()).await?;
    debug!("connected to {}", &addr);

    let repo_id_request = tonic::Request::new(RepositoryIdRequest {});
    let RepositoryIdResponse {
        repo_id: remote_repo_id,
    } = client.repository_id(repo_id_request).await?.into_inner();
    info!("remote repo_id={}", remote_repo_id);

    let db_clone = db.clone();
    let mut missing_blobs = db_clone.missing_blobs(remote_repo_id.clone(), local_repo_id.clone());
    while let Some(next) = missing_blobs.next().await {
        let BlobObjectId { object_id } = next?;
        let content = client
            .download(DownloadRequest {
                object_id: object_id.clone(),
            })
            .await?
            .into_inner()
            .content;

        let blob_path = invariable_path.join("blobs");
        fs::create_dir_all(blob_path.as_path()).await?;
        let object_path = blob_path.join(&object_id);

        let mut file = File::create(&object_path).await?;
        file.write_all(&content).await?;
        file.sync_all().await?;

        db.add_blob(&local_repo_id, &object_id, chrono::Utc::now(), true)
            .await?;
        debug!("added blob {:?}", object_path);
    }
    debug!("downloaded all blobs");

    reconcile_filesystem(db, current_path).await?;

    Ok(())
}

async fn reconcile_filesystem(db: DB, root: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let CurrentRepository { repo_id } = db.get_or_create_current_repository().await?;

    let mut desired_state = db.desired_filesystem_state(repo_id.clone());
    while let Some(next) = desired_state.next().await {
        let FilePathWithObjectId { path: relative_path, object_id } = next?;
        let invariable_path = root.join(".inv");
        let blob_path = invariable_path.join("blobs");
        let object_path = blob_path.join(object_id);

        let target_path = root.join(relative_path);
        debug!("trying hardlinking {:?} -> {:?}", object_path, target_path);

        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        if !fs::metadata(&target_path)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            fs::hard_link(&object_path, &target_path).await.context("unable to hardlink files")?;
            debug!("hardlinked {:?} -> {:?}", object_path, target_path);
        } else {
            debug!("skipped hardlinked {:?} -> {:?}", object_path, target_path);
        };
    }

    debug!("reconciling filesystem");

    Ok(())
}
