use crate::db::models::Blob as DbBlob;
use crate::db::models::File as DbFile;
use crate::db::models::Repository as DbRepository;
use crate::repository::grpc::GRPCClient;
use crate::repository::local_repository::LocalRepository;
use crate::repository::logic::sync;
use crate::repository::traits::{LastIndicesSyncer, Metadata};
use anyhow::Result;
use log::{debug, info};

pub async fn sync(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let local = LocalRepository::new(None).await?;
    let remote = GRPCClient::connect(format!("http://127.0.0.1:{}", port)).await?;

    let local_repo_id = local.repo_id().await?;
    debug!("local repo_id={}", local_repo_id);

    let remote_repo = remote.repo_id().await?;
    info!("remote repo_id={}", remote_repo);

    let remote_last_indices = remote.lookup(local_repo_id).await?;
    debug!("remote remote_last_indices={:?}", remote_last_indices);

    let local_last_indices = local.lookup(remote_repo).await?;
    debug!("local local_last_indices={:?}", local_last_indices);

    sync::sync_table::<DbFile, _, _>(
        local.clone(),
        local_last_indices.file,
        remote.clone(),
        remote_last_indices.file,
    )
    .await?;
    sync::sync_table::<DbBlob, _, _>(
        local.clone(),
        local_last_indices.blob,
        remote.clone(),
        remote_last_indices.blob,
    )
    .await?;

    remote.refresh().await?;
    debug!("remote: updated last indices");

    local.refresh().await?;
    debug!("local: updated last indices");

    sync::sync_table::<DbRepository, _, _>(local, (), remote, ()).await?;

    Ok(())
}
