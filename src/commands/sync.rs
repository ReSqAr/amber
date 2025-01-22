use crate::db::models::{Blob, File, Repository};
use crate::repository::local::LocalRepository;
use crate::repository::logic::sync;
use crate::repository::traits::{ConnectionManager, LastIndicesSyncer, Metadata, Syncer};
use log::debug;

pub async fn sync(name: String) -> Result<(), Box<dyn std::error::Error>> {
    let local = LocalRepository::new(None).await?;
    let remote = local.connect(name).await?.repository;

    if let Some(tracked_remote) = remote.as_tracked() {
        sync_repositories(local, tracked_remote).await?;
    }

    Ok(())
}

pub async fn sync_repositories<S, T>(local: S, remote: T) -> Result<(), Box<dyn std::error::Error>>
where
    S: Metadata
        + LastIndicesSyncer
        + Syncer<Repository>
        + Syncer<File>
        + Syncer<Blob>
        + Clone
        + Send
        + Sync,
    T: Metadata
        + LastIndicesSyncer
        + Syncer<Repository>
        + Syncer<File>
        + Syncer<Blob>
        + Clone
        + Send
        + Sync,
{
    let local_repo_id = local.repo_id().await?;
    let remote_repo_id = remote.repo_id().await?;
    debug!(
        "repo_id - local: {} remote: {}",
        local_repo_id, remote_repo_id
    );

    let remote_last_indices = remote.lookup(local_repo_id).await?;
    let local_last_indices = local.lookup(remote_repo_id).await?;
    debug!(
        "remote_last_indices - local: {:?} remote: {:?}",
        local_last_indices, remote_last_indices
    );

    sync::sync_table::<File, _, _>(
        local.clone(),
        local_last_indices.file,
        remote.clone(),
        remote_last_indices.file,
    )
    .await?;
    sync::sync_table::<Blob, _, _>(
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

    sync::sync_table::<Repository, _, _>(local, (), remote, ()).await?;

    Ok(())
}
