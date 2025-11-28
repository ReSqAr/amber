use crate::db::models::{Blob, File, Repository, RepositoryName};
use crate::flightdeck::base::BaseObserver;
use crate::repository::traits::{LastIndicesSyncer, Metadata, Syncer, SyncerParams};
use crate::utils::errors::InternalError;
use crate::utils::pipe::TryForwardIntoExt;
use futures::{StreamExt, try_join};
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
    let local_stream = local.select(local_param).await;
    let remote_stream = remote.select(remote_param).await;

    try_join!(
        async {
            local_stream
                .try_forward_into::<_, _, _, _, InternalError>(|s| remote.merge(s.boxed()))
                .await?;
            debug!("remote: merged type: {}", std::any::type_name::<I>());
            Ok::<_, InternalError>(())
        },
        async {
            remote_stream
                .try_forward_into::<_, _, _, _, InternalError>(|s| local.merge(s.boxed()))
                .await?;
            debug!("local: merged type: {}", std::any::type_name::<I>());
            Ok::<_, InternalError>(())
        },
    )?;

    Ok(())
}

pub async fn sync_repositories<S, T>(local: &S, remote: &T) -> Result<(), InternalError>
where
    S: Metadata
        + LastIndicesSyncer
        + Syncer<Repository>
        + Syncer<File>
        + Syncer<Blob>
        + Syncer<RepositoryName>
        + Clone
        + Send
        + Sync,
    T: Metadata
        + LastIndicesSyncer
        + Syncer<Repository>
        + Syncer<File>
        + Syncer<Blob>
        + Syncer<RepositoryName>
        + Clone
        + Send
        + Sync,
{
    let local_meta = local.current().await?;
    let remote_meta = remote.current().await?;

    let local_last_indices = remote.lookup(local_meta.id).await?;
    let remote_last_indices = local.lookup(remote_meta.id).await?;

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
        let mut o = BaseObserver::with_id("sync:table", "repository names");
        sync_table::<RepositoryName, _, _>(
            local,
            local_last_indices.name,
            remote,
            remote_last_indices.name,
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
