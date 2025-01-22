use crate::db::models::Blob as DbBlob;
use crate::db::models::File as DbFile;
use crate::db::models::Repository as DbRepository;
use crate::db::models::TransferItem as DbTransferItem;
use crate::grpc::server::grpc::grpc_client::GrpcClient;
use crate::grpc::server::grpc::{
    Blob, CreateTransferRequestRequest, File, FinaliseTransferRequest, LookupLastIndicesRequest,
    LookupLastIndicesResponse, Repository, RepositoryIdRequest, SelectBlobsRequest,
    SelectFilesRequest, SelectRepositoriesRequest, TransferItem, UpdateLastIndicesRequest,
};
use crate::repository::traits::{
    BlobReceiver, BlobSender, LastIndices, LastIndicesSyncer, Metadata, Syncer,
};
use crate::utils::app_error::AppError;
use futures::TryStreamExt;
use futures::{FutureExt, TryFutureExt};
use futures::{Stream, StreamExt};
use log::debug;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub(crate) struct GRPCClient {
    client: Arc<RwLock<GrpcClient<tonic::transport::Channel>>>,
}

impl GRPCClient {
    pub fn new(client: GrpcClient<tonic::transport::Channel>) -> Self {
        Self {
            client: Arc::new(client.into()),
        }
    }

    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        debug!("connecting to {}", &addr);
        let client = GrpcClient::connect(addr.clone()).await?;
        debug!("connected to {}", &addr);

        Ok(Self::new(client))
    }
}

impl Metadata for GRPCClient {
    async fn repo_id(&self) -> Result<String, AppError> {
        let repo_id_request = tonic::Request::new(RepositoryIdRequest {});
        let remote_repo = self
            .client
            .write()
            .await
            .repository_id(repo_id_request)
            .await?
            .into_inner();
        Ok(remote_repo.repo_id)
    }
}

impl LastIndicesSyncer for GRPCClient {
    async fn lookup(&self, repo_id: String) -> Result<LastIndices, AppError> {
        let LookupLastIndicesResponse { file, blob } = self
            .client
            .write()
            .await
            .lookup_last_indices(LookupLastIndicesRequest {
                repo_id: repo_id.clone(),
            })
            .await?
            .into_inner();
        Ok(LastIndices { file, blob })
    }

    async fn refresh(&self) -> Result<(), AppError> {
        self.client
            .write()
            .await
            .update_last_indices(UpdateLastIndicesRequest {})
            .await?;
        Ok(())
    }
}

impl Syncer<DbRepository> for GRPCClient {
    fn select(
        &self,
        _params: (),
    ) -> impl std::future::Future<
        Output = impl futures::Stream<Item = Result<DbRepository, AppError>> + Unpin + Send + 'static,
    > {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .select_repositories(SelectRepositoriesRequest {})
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into().map_ok(DbRepository::from))
                .await
        }
    }

    fn merge<S>(&self, s: S) -> impl std::future::Future<Output = Result<(), AppError>> + Send
    where
        S: futures::Stream<Item = DbRepository> + Unpin + Send + 'static,
    {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .merge_repositories(s.map(Repository::from))
                .err_into()
                .map_ok(|_| ())
                .await
        }
    }
}

impl Syncer<DbFile> for GRPCClient {
    fn select(
        &self,
        last_index: i32,
    ) -> impl std::future::Future<
        Output = impl futures::Stream<Item = Result<DbFile, AppError>> + Unpin + Send + 'static,
    > {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .select_files(SelectFilesRequest { last_index })
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into().map_ok(DbFile::from))
                .await
        }
    }

    fn merge<S>(&self, s: S) -> impl std::future::Future<Output = Result<(), AppError>> + Send
    where
        S: futures::Stream<Item = DbFile> + Unpin + Send + 'static,
    {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .merge_files(s.map(File::from))
                .err_into()
                .map_ok(|_| ())
                .await
        }
    }
}

impl Syncer<DbBlob> for GRPCClient {
    fn select(
        &self,
        last_index: i32,
    ) -> impl std::future::Future<
        Output = impl futures::Stream<Item = Result<DbBlob, AppError>> + Unpin + Send + 'static,
    > {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .select_blobs(SelectBlobsRequest { last_index })
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into().map_ok(DbBlob::from))
                .await
        }
    }

    fn merge<S>(&self, s: S) -> impl std::future::Future<Output = Result<(), AppError>> + Send
    where
        S: futures::Stream<Item = DbBlob> + Unpin + Send + 'static,
    {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .merge_blobs(s.map(Blob::from))
                .err_into()
                .map_ok(|_| ())
                .await
        }
    }
}

impl BlobSender for GRPCClient {
    fn prepare_transfer<S>(
        &self,
        s: S,
    ) -> impl std::future::Future<Output = Result<(), AppError>> + Send
    where
        S: Stream<Item = DbTransferItem> + Unpin + Send + 'static,
    {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .prepare_transfer(s.map(TransferItem::from))
                .err_into()
                .map_ok(|_| ())
                .await
        }
    }
}

impl BlobReceiver for GRPCClient {
    fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: String,
    ) -> impl std::future::Future<
        Output = impl Stream<Item = Result<DbTransferItem, AppError>> + Unpin + Send + 'static,
    > {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .create_transfer_request(CreateTransferRequestRequest {
                    transfer_id,
                    repo_id,
                })
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into().map_ok(DbTransferItem::from))
                .await
        }
    }

    async fn finalise_transfer(&self, transfer_id: u32) -> Result<(), AppError> {
        self.client
            .write()
            .await
            .finalise_transfer(tonic::Request::new(FinaliseTransferRequest { transfer_id }))
            .await?
            .into_inner();
        Ok(())
    }
}
