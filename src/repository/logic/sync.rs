use crate::repository::traits::{Syncer, SyncerParams};
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
