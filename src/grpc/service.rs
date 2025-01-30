use crate::db;
use crate::grpc::definitions::{
    grpc_server, Blob, CreateTransferRequestRequest, File, FinaliseTransferRequest,
    FinaliseTransferResponse, LookupLastIndicesRequest, LookupLastIndicesResponse,
    MergeBlobsResponse, MergeFilesResponse, MergeRepositoriesResponse, PrepareTransferResponse,
    Repository, RepositoryIdRequest, RepositoryIdResponse, SelectBlobsRequest, SelectFilesRequest,
    SelectRepositoriesRequest, TransferItem, UpdateLastIndicesRequest, UpdateLastIndicesResponse,
};
use crate::repository::traits::{
    Adder, BlobReceiver, BlobSender, LastIndices, LastIndicesSyncer, Local, Metadata, Syncer,
};
use crate::utils::errors::InternalError;
use crate::utils::pipe::TryForwardIntoExt;
use db::models::Blob as DbBlob;
use db::models::File as DbFile;
use db::models::Repository as DbRepository;
use db::models::TransferItem as DbTransferItem;
use futures::Stream;
use futures::TryStreamExt;
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};

pub struct Service<T> {
    repository: T,
}

impl<T> Service<T> {
    pub fn new(repository: T) -> Self {
        Self { repository }
    }
}

#[tonic::async_trait]
impl<T> grpc_server::Grpc for Service<T>
where
    T: Metadata
        + Local
        + Adder
        + LastIndicesSyncer
        + Syncer<DbRepository>
        + Syncer<DbFile>
        + Syncer<DbBlob>
        + BlobSender
        + BlobReceiver
        + Sync
        + Send
        + 'static,
{
    async fn repository_id(
        &self,
        _: Request<RepositoryIdRequest>,
    ) -> Result<Response<RepositoryIdResponse>, Status> {
        let repo_id = self.repository.repo_id().await?;
        Ok(Response::new(RepositoryIdResponse { repo_id }))
    }

    async fn merge_repositories(
        &self,
        request: Request<Streaming<Repository>>,
    ) -> Result<Response<MergeRepositoriesResponse>, Status> {
        request
            .into_inner()
            .map_ok::<DbRepository, _>(Repository::into)
            .try_forward_into::<_, _, _, _, InternalError>(|s| self.repository.merge(s))
            .await?;
        Ok(Response::new(MergeRepositoriesResponse {}))
    }
    async fn merge_files(
        &self,
        request: Request<Streaming<File>>,
    ) -> Result<Response<MergeFilesResponse>, Status> {
        request
            .into_inner()
            .map_ok::<DbFile, _>(File::into)
            .try_forward_into::<_, _, _, _, InternalError>(|s| self.repository.merge(s))
            .await?;
        Ok(Response::new(MergeFilesResponse {}))
    }
    async fn merge_blobs(
        &self,
        request: Request<Streaming<Blob>>,
    ) -> Result<Response<MergeBlobsResponse>, Status> {
        request
            .into_inner()
            .map_ok::<DbBlob, _>(Blob::into)
            .try_forward_into::<_, _, _, _, InternalError>(|s| self.repository.merge(s))
            .await?;
        Ok(Response::new(MergeBlobsResponse {}))
    }

    async fn update_last_indices(
        &self,
        _: Request<UpdateLastIndicesRequest>,
    ) -> Result<Response<UpdateLastIndicesResponse>, Status> {
        self.repository.refresh().await?;
        Ok(Response::new(UpdateLastIndicesResponse {}))
    }

    async fn lookup_last_indices(
        &self,
        request: Request<LookupLastIndicesRequest>,
    ) -> Result<Response<LookupLastIndicesResponse>, Status> {
        let LastIndices { file, blob } =
            self.repository.lookup(request.into_inner().repo_id).await?;
        Ok(Response::new(LookupLastIndicesResponse { file, blob }))
    }

    type SelectRepositoriesStream =
        Pin<Box<dyn Stream<Item = Result<Repository, Status>> + Send + 'static>>;

    async fn select_repositories(
        &self,
        _: Request<SelectRepositoriesRequest>,
    ) -> Result<Response<Self::SelectRepositoriesStream>, Status> {
        let stream = <T as Syncer<DbRepository>>::select(&self.repository, ())
            .await
            .err_into()
            .map_ok::<Repository, _>(DbRepository::into);
        Ok(Response::new(Box::pin(stream)))
    }

    type SelectFilesStream = Pin<Box<dyn Stream<Item = Result<File, Status>> + Send + 'static>>;

    async fn select_files(
        &self,
        request: Request<SelectFilesRequest>,
    ) -> Result<Response<Self::SelectFilesStream>, Status> {
        let last_index = request.into_inner().last_index;
        let stream = <T as Syncer<DbFile>>::select(&self.repository, last_index)
            .await
            .err_into()
            .map_ok::<File, _>(DbFile::into);
        Ok(Response::new(Box::pin(stream)))
    }

    type SelectBlobsStream = Pin<Box<dyn Stream<Item = Result<Blob, Status>> + Send + 'static>>;

    async fn select_blobs(
        &self,
        request: Request<SelectBlobsRequest>,
    ) -> Result<Response<Self::SelectBlobsStream>, Status> {
        let last_index = request.into_inner().last_index;
        let stream = <T as Syncer<DbBlob>>::select(&self.repository, last_index)
            .await
            .err_into()
            .map_ok::<Blob, _>(DbBlob::into);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn prepare_transfer(
        &self,
        request: Request<Streaming<TransferItem>>,
    ) -> Result<Response<PrepareTransferResponse>, Status> {
        request
            .into_inner()
            .map_ok::<DbTransferItem, _>(TransferItem::into)
            .try_forward_into::<_, _, _, _, InternalError>(|s| self.repository.prepare_transfer(s))
            .await?;
        Ok(Response::new(PrepareTransferResponse {}))
    }

    type CreateTransferRequestStream =
        Pin<Box<dyn Stream<Item = Result<TransferItem, Status>> + Send + 'static>>;

    async fn create_transfer_request(
        &self,
        request: Request<CreateTransferRequestRequest>,
    ) -> Result<Response<Self::CreateTransferRequestStream>, Status> {
        let CreateTransferRequestRequest {
            transfer_id,
            repo_id,
        } = request.into_inner();
        let stream = self
            .repository
            .create_transfer_request(transfer_id, repo_id)
            .await
            .err_into()
            .map_ok::<TransferItem, _>(DbTransferItem::into);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn finalise_transfer(
        &self,
        request: Request<FinaliseTransferRequest>,
    ) -> Result<Response<FinaliseTransferResponse>, Status> {
        let FinaliseTransferRequest { transfer_id } = request.into_inner();
        self.repository.finalise_transfer(transfer_id).await?;
        Ok(Response::new(FinaliseTransferResponse {}))
    }
}
