use crate::db::models::Blob as DbBlob;
use crate::db::models::File as DbFile;
use crate::db::models::Repository as DbRepository;
use crate::repository::grpc::GRPCClient;
use crate::repository::local_repository::LocalRepository;
use crate::repository::logic::sync;
use crate::repository::traits::{LastIndicesSyncer, Metadata, Syncer};
use anyhow::Result;
use log::debug;

pub async fn sync(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let local = LocalRepository::new(None).await?;
    let remote = GRPCClient::connect(format!("http://127.0.0.1:{}", port)).await?;

    sync_repositories(local, remote).await
}

pub async fn sync_repositories<S, T>(local: S, remote: T) -> Result<(), Box<dyn std::error::Error>>
where
    S: Metadata
        + Syncer<DbRepository>
        + Syncer<DbFile>
        + Syncer<DbBlob>
        + Clone
        + LastIndicesSyncer
        + Send
        + Sync,
    T: Metadata
        + Syncer<DbRepository>
        + Syncer<DbFile>
        + Syncer<DbBlob>
        + Clone
        + LastIndicesSyncer
        + Send
        + Sync,
{
    let local_repo_id = local.repo_id().await?;
    let remote_repo = remote.repo_id().await?;
    debug!(
        "repo_id  - local: {} remote: {}",
        local_repo_id, remote_repo
    );

    let remote_last_indices = remote.lookup(local_repo_id).await?;
    let local_last_indices = local.lookup(remote_repo).await?;
    debug!(
        "remote_last_indices - local: {:?} remote: {:?}",
        local_last_indices, remote_last_indices
    );

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
