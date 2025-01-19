use crate::db::models::Blob as DbBlob;
use crate::db::models::Repository as DbRepository;
use crate::db::models::File as DbFile;
use crate::repository::local_repository::{
    LastIndicesSyncer, LocalRepository, Metadata, Syncer, SyncerParams,
};
use crate::transport::client::Client;
use crate::transport::server::invariable::Blob as GRPCBlob;
use crate::transport::server::invariable::File as GRPCFile;
use crate::transport::server::invariable::Repository as GRPCRepository;
use crate::utils::app_error::AppError;
use crate::utils::pipe::TryForwardIntoExt;
use anyhow::Result;
use futures::TryStreamExt;
use log::{debug, info};

pub async fn sync(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let local = LocalRepository::new(None).await?;

    let remote = Client::connect(format!("http://127.0.0.1:{}", port)).await?;

    let local_repo_id = local.repo_id().await?;
    debug!("local repo_id={}", local_repo_id);

    let remote_repo = remote.repo_id().await?;
    info!("remote repo_id={}", remote_repo);

    let remote_last_indices = remote.lookup(local_repo_id).await?;
    debug!("remote remote_last_indices={:?}", remote_last_indices);

    let local_last_indices = local.lookup(remote_repo).await?;
    debug!("local local_last_indices={:?}", local_last_indices);

    sync_table::<DbFile, GRPCFile, _, _>(
        local.clone(),
        local_last_indices.file,
        remote.clone(),
        remote_last_indices.file,
    )
    .await?;
    sync_table::<DbBlob, GRPCBlob, _, _>(
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

    sync_table::<DbRepository, GRPCRepository, _, _>(local, (), remote, ()).await?;

    Ok(())
}

async fn sync_table<LI, RI, L, R>(
    local: L,
    local_param: LI::Params,
    remote: R,
    remote_param: RI::Params,
) -> Result<(), Box<dyn std::error::Error>>
where
    LI: From<RI> + Send + Sync + 'static + SyncerParams,
    RI: From<LI> + Send + Sync + 'static + SyncerParams,
    L: Syncer<LI> + Send + Sync,
    R: Syncer<RI> + Send + Sync,
{
    local
        .select(local_param)
        .await
        .map_ok(RI::from)
        .try_forward_into::<_, _, _, _, AppError>(|s| remote.merge(s))
        .await?;
    debug!(
        "remote: merged repositories (type: {})",
        std::any::type_name::<RI>()
    );

    remote
        .select(remote_param)
        .await
        .map_ok(LI::from)
        .try_forward_into::<_, _, _, _, AppError>(|s| local.merge(s))
        .await?;
    debug!(
        "local: merged repositories (type: {})",
        std::any::type_name::<LI>()
    );

    Ok(())
}
