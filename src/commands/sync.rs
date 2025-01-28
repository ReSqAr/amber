use crate::db::models::{Blob, File, Repository};
use crate::repository::local::LocalRepository;
use crate::repository::logic::{checkout, sync};
use crate::repository::traits::{ConnectionManager, LastIndicesSyncer, Metadata, Syncer};
use crate::utils::errors::AppError;
use log::debug;

pub async fn sync(connection_name: String) -> Result<(), Box<dyn std::error::Error>> {
    let local = LocalRepository::new(None).await?;
    let connection = local.connect(connection_name.clone()).await?;
    let managed_remote = match connection.remote.as_managed() {
        Some(tracked_remote) => tracked_remote,
        None => {
            return Err(AppError::UnsupportedRemote(format!(
                "{} does not support sync",
                connection_name
            ))
            .into());
        }
    };

    sync_repositories(&local, &managed_remote).await?;

    checkout::checkout(&local).await?;

    Ok(())
}

pub async fn sync_repositories<S, T>(
    local: &S,
    remote: &T,
) -> Result<(), Box<dyn std::error::Error>>
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
        local,
        local_last_indices.file,
        remote,
        remote_last_indices.file,
    )
    .await?;
    sync::sync_table::<Blob, _, _>(
        local,
        local_last_indices.blob,
        remote,
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
