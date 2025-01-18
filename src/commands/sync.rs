use crate::db::models::{Repository as DbRepository, Repository};
use crate::transport::server::invariable::invariable_client::InvariableClient;
use crate::transport::server::invariable::Blob as GRPCBlob;
use crate::transport::server::invariable::File as GRPCFile;
use crate::transport::server::invariable::LookupRepositoryRequest;
use crate::transport::server::invariable::Repository as GRPCRepository;
use crate::transport::server::invariable::RepositoryIdRequest;
use crate::transport::server::invariable::SelectBlobsRequest;
use crate::transport::server::invariable::SelectFilesRequest;
use crate::transport::server::invariable::SelectRepositoriesRequest;
use crate::transport::server::invariable::UpdateLastIndicesRequest;
use crate::utils::app_error::AppError;
use crate::utils::pipe::TryForwardIntoExt;
use anyhow::Result;
use futures::TryStreamExt;
use log::{debug, info};
use tonic::transport::Channel;
use crate::repository::local_repository::{LocalRepository, Syncer, SyncerParams};

pub async fn sync(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    let local_repo = local_repository.db.get_or_create_current_repository().await?;
    debug!("local repo_id={}", local_repo.repo_id);

    let addr = format!("http://127.0.0.1:{}", port);
    debug!("connecting to {}", &addr);
    let mut client = InvariableClient::connect(addr.clone()).await?;
    debug!("connected to {}", &addr);

    let repo_id_request = tonic::Request::new(RepositoryIdRequest {});
    let remote_repo = client.repository_id(repo_id_request).await?.into_inner();
    info!("remote repo_id={}", remote_repo.repo_id);

    let lookup_repo_request = tonic::Request::new(LookupRepositoryRequest {
        repo_id: local_repo.repo_id.clone(),
    });
    let GRPCRepository {
        last_file_index: remote_last_file_index,
        last_blob_index: remote_last_blob_index,
        ..
    } = client
        .lookup_repository(lookup_repo_request)
        .await?
        .into_inner()
        .repo
        .unwrap_or(GRPCRepository {
            repo_id: local_repo.repo_id.clone(),
            last_file_index: -1,
            last_blob_index: -1,
        });
    debug!(
        "remote remote_last_file_index={} remote_last_blob_index={}",
        remote_last_file_index, remote_last_blob_index
    );

    local_repository.db.select_files(remote_last_file_index).await
        .map_ok(GRPCFile::from)
        .try_forward_into::<_, _, _, _, AppError>(|s| client.merge_files(s))
        .await?;
    debug!("remote: merged files");

    local_repository.db.select_blobs(remote_last_blob_index)
        .await.map_ok(GRPCBlob::from)
        .try_forward_into::<_, _, _, _, AppError>(|s| client.merge_blobs(s))
        .await?;
    debug!("remote: merged blobs");

    let DbRepository {
        last_file_index: local_last_file_index,
        last_blob_index: local_last_blob_index,
        ..
    } = local_repository.db.lookup_repository(remote_repo.repo_id).await?;
    debug!(
        "local local_last_file_index={} local_last_blob_index={}",
        local_last_file_index, local_last_blob_index
    );

    client
        .select_files(SelectFilesRequest {
            last_index: local_last_file_index,
        })
        .await?
        .into_inner()
        .map_ok(GRPCFile::into)
        .try_forward_into::<_, _, _, _, AppError>(|s| local_repository.db.merge_files(s))
        .await?;
    debug!("local: merged files");

    client
        .select_blobs(SelectBlobsRequest {
            last_index: local_last_blob_index,
        })
        .await?
        .into_inner()
        .map_ok(GRPCBlob::into)
        .try_forward_into::<_, _, _, _, AppError>(|s| local_repository.db.merge_blobs(s))
        .await?;
    debug!("local: merged blobs");

    client
        .update_last_indices(UpdateLastIndicesRequest {})
        .await?;
    debug!("remote: updated last indices");

    local_repository.db.update_last_indices().await?;
    debug!("local: updated last indices");


    sync_table::<Repository, _>(local_repository, (), client).await?;

    Ok(())
}

async fn sync_table<LI, L>(local: L, local_param: LI::Params, mut remote: InvariableClient<Channel>) -> Result<(), Box<dyn std::error::Error>>
where
    LI: From<GRPCRepository> + Send + Sync + 'static + SyncerParams,
    L: Syncer<LI> + Send + Sync,
    GRPCRepository: From<LI>,
{
    local.select(local_param)
        .await
        .map_ok(GRPCRepository::from)
        .try_forward_into::<_, _, _, _, AppError>(|s| remote.merge_repositories(s))
        .await?;
    debug!("remote: merged repositories");

    remote
        .select_repositories(SelectRepositoriesRequest {})
        .await?
        .into_inner()
        .map_ok(GRPCRepository::into)
        .try_forward_into::<_, _, _, _, AppError>(|s| local.merge(s))
        .await?;
    debug!("local: merged repositories");

    Ok(())
}
