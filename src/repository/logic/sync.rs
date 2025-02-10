use crate::db::models::{Blob, File, Repository};
use crate::flightdeck::base::BaseObserver;
use crate::repository::traits::{LastIndicesSyncer, Metadata, Syncer, SyncerParams};
use crate::utils::errors::InternalError;
use crate::utils::pipe::TryForwardIntoExt;
use log::debug;

pub async fn sync_table<I, L, R>(
    local: &L,
    local_param: I::Params,
    remote: &R,
    remote_param: I::Params,
) -> Result<(), InternalError>
where
    I: Send + Sync + 'static + SyncerParams,
    L: Syncer<I> + Send + Sync,
    R: Syncer<I> + Send + Sync,
{
    local
        .select(local_param)
        .await
        .try_forward_into::<_, _, _, _, InternalError>(|s| remote.merge(s))
        .await?;
    debug!("remote: merged type: {}", std::any::type_name::<I>());

    remote
        .select(remote_param)
        .await
        .try_forward_into::<_, _, _, _, InternalError>(|s| local.merge(s))
        .await?;
    debug!("local: merged type: {}", std::any::type_name::<I>());

    Ok(())
}

pub async fn sync_repositories<S, T>(local: &S, remote: &T) -> Result<(), InternalError>
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

    let local_last_indices = remote.lookup(local_repo_id).await?;
    let remote_last_indices = local.lookup(remote_repo_id).await?;

    {
        let mut o = BaseObserver::with_id("sync:table", "files");
        sync_table::<File, _, _>(
            local,
            local_last_indices.file,
            remote,
            remote_last_indices.file,
        )
        .await?;
        o.observe_termination(log::Level::Info, "synchronised");
    }

    {
        let mut o = BaseObserver::with_id("sync:table", "blobs");
        sync_table::<Blob, _, _>(
            local,
            local_last_indices.blob,
            remote,
            remote_last_indices.blob,
        )
        .await?;
        o.observe_termination(log::Level::Info, "synchronised");
    }

    {
        let mut o = BaseObserver::with_id("sync:table", "repositories");
        remote.refresh().await?;
        local.refresh().await?;
        o.observe_state(log::Level::Info, "prepared");

        sync_table::<Repository, _, _>(local, (), remote, ()).await?;
        o.observe_termination(log::Level::Info, "synchronised");
    }

    Ok(())
}
