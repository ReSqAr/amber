use crate::db::models::{BlobId, FilePathWithBlobId, InsertBlob};
use crate::transport::server::invariable::invariable_client::InvariableClient;
use crate::transport::server::invariable::{
    DownloadRequest, RepositoryIdRequest, RepositoryIdResponse,
};
use anyhow::{Context, Result};
use futures::{stream, StreamExt};
use log::{debug, info};
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use crate::repository::local_repository::LocalRepository;
use crate::repository::traits::{Adder, Deprecated, Local, Metadata, Reconciler};

pub async fn pull(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;
    let local_repo_id = local_repository.repo_id().await?;
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

    let mut missing_blobs = local_repository.missing_blobs(remote_repo_id.clone(), local_repo_id.clone());
    while let Some(next) = missing_blobs.next().await {
        let BlobId { blob_id } = next?;
        let content = client
            .download(DownloadRequest {
                blob_id: blob_id.clone(),
            })
            .await?
            .into_inner()
            .content;

        let blob_path = local_repository.blob_path();
        fs::create_dir_all(blob_path.as_path()).await?;
        let object_path = blob_path.join(&blob_id);

        let mut file = File::create(&object_path).await?;
        file.write_all(&content).await?;
        file.sync_all().await?;

        let b = InsertBlob {
            repo_id: local_repo_id.clone(),
            blob_id,
            blob_size: content.len() as i64,
            has_blob: true,
            valid_from: chrono::Utc::now(),
        };
        let sb = stream::iter(vec![b]);
        local_repository.add_blobs(sb).await?;

        debug!("added blob {:?}", object_path);
    }
    debug!("downloaded all blobs");

    reconcile_filesystem(&local_repository).await?;

    Ok(())
}

async fn reconcile_filesystem(local_repository: &LocalRepository) -> Result<(), Box<dyn std::error::Error>> {
    let mut desired_state = local_repository.target_filesystem_state();
    while let Some(next) = desired_state.next().await {
        let FilePathWithBlobId {
            path: relative_path,
            blob_id,
        } = next?;
        let invariable_path = local_repository.root().join(".inv");
        let blob_path = invariable_path.join("blobs");
        let object_path = blob_path.join(blob_id);

        let target_path = local_repository.root().join(relative_path);
        debug!("trying hardlinking {:?} -> {:?}", object_path, target_path);

        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        if !fs::metadata(&target_path)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            fs::hard_link(&object_path, &target_path)
                .await
                .context("unable to hardlink files")?;
            debug!("hardlinked {:?} -> {:?}", object_path, target_path);
        } else {
            debug!("skipped hardlinked {:?} -> {:?}", object_path, target_path);
        };
    }

    debug!("reconciling filesystem");

    Ok(())
}
