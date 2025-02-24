use crate::db::models;
use crate::grpc::auth::ClientAuth;
use crate::grpc::definitions::grpc_client::GrpcClient;
use crate::grpc::definitions::{
    Blob, CreateTransferRequestRequest, CurrentRepositoryMetadataRequest, File,
    FinaliseTransferRequest, FinaliseTransferResponse, LookupLastIndicesRequest,
    LookupLastIndicesResponse, PrepareTransferResponse, RclonePathRequest, RclonePathResponse,
    Repository, RepositoryName, SelectBlobsRequest, SelectFilesRequest, SelectRepositoriesRequest,
    SelectRepositoryNamesRequest, TransferItem, UpdateLastIndicesRequest,
};
use crate::repository::traits::{
    LastIndices, LastIndicesSyncer, Metadata, RcloneTargetPath, Receiver, RepositoryMetadata,
    Sender, Syncer,
};
use crate::utils::errors::InternalError;
use futures::TryStreamExt;
use futures::{FutureExt, TryFutureExt};
use futures::{Stream, StreamExt};
use log::debug;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;

#[derive(Clone)]
pub(crate) struct GRPCClient {
    client: Arc<RwLock<GrpcClient<InterceptedService<Channel, ClientAuth>>>>,
}

impl GRPCClient {
    pub fn new(client: GrpcClient<InterceptedService<Channel, ClientAuth>>) -> Self {
        Self {
            client: Arc::new(client.into()),
        }
    }

    pub async fn connect(addr: String, auth_key: String) -> Result<Self, InternalError> {
        debug!("connecting to {}", &addr);

        let channel = tonic::transport::Endpoint::from_shared(addr.clone())?
            .connect()
            .await?;

        let interceptor = ClientAuth::new(&auth_key).map_err(|e| {
            InternalError::Grpc(format!("unable to create authentication method: {e}"))
        })?;
        let client = GrpcClient::with_interceptor(channel, interceptor);

        debug!("connected to {}", &addr);
        Ok(Self::new(client))
    }
}

impl Metadata for GRPCClient {
    async fn current(&self) -> Result<RepositoryMetadata, InternalError> {
        let repo_id_request = tonic::Request::new(CurrentRepositoryMetadataRequest {});
        let meta = self
            .client
            .write()
            .await
            .current_repository_metadata(repo_id_request)
            .await?
            .into_inner();
        Ok(RepositoryMetadata {
            id: meta.id,
            name: meta.name,
        })
    }
}

impl LastIndicesSyncer for GRPCClient {
    async fn lookup(&self, repo_id: String) -> Result<LastIndices, InternalError> {
        let LookupLastIndicesResponse { file, blob, name } = self
            .client
            .write()
            .await
            .lookup_last_indices(LookupLastIndicesRequest {
                repo_id: repo_id.clone(),
            })
            .await?
            .into_inner();
        Ok(LastIndices { file, blob, name })
    }

    async fn refresh(&self) -> Result<(), InternalError> {
        self.client
            .write()
            .await
            .update_last_indices(UpdateLastIndicesRequest {})
            .await?;
        Ok(())
    }
}

impl Syncer<models::Repository> for GRPCClient {
    fn select(
        &self,
        _params: (),
    ) -> impl std::future::Future<
        Output = impl futures::Stream<Item = Result<models::Repository, InternalError>>
                 + Unpin
                 + Send
                 + 'static,
    > {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .select_repositories(SelectRepositoriesRequest {})
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into().map_ok(models::Repository::from))
                .await
        }
    }

    fn merge<S>(&self, s: S) -> impl std::future::Future<Output = Result<(), InternalError>> + Send
    where
        S: futures::Stream<Item = models::Repository> + Unpin + Send + 'static,
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

impl Syncer<models::File> for GRPCClient {
    fn select(
        &self,
        last_index: i32,
    ) -> impl std::future::Future<
        Output = impl futures::Stream<Item = Result<models::File, InternalError>>
                 + Unpin
                 + Send
                 + 'static,
    > {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .select_files(SelectFilesRequest { last_index })
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into().map_ok(models::File::from))
                .await
        }
    }

    fn merge<S>(&self, s: S) -> impl std::future::Future<Output = Result<(), InternalError>> + Send
    where
        S: futures::Stream<Item = models::File> + Unpin + Send + 'static,
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

impl Syncer<models::Blob> for GRPCClient {
    fn select(
        &self,
        last_index: i32,
    ) -> impl std::future::Future<
        Output = impl futures::Stream<Item = Result<models::Blob, InternalError>>
                 + Unpin
                 + Send
                 + 'static,
    > {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .select_blobs(SelectBlobsRequest { last_index })
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into().map_ok(models::Blob::from))
                .await
        }
    }

    fn merge<S>(&self, s: S) -> impl std::future::Future<Output = Result<(), InternalError>> + Send
    where
        S: futures::Stream<Item = models::Blob> + Unpin + Send + 'static,
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

impl Syncer<models::RepositoryName> for GRPCClient {
    fn select(
        &self,
        last_index: i32,
    ) -> impl std::future::Future<
        Output = impl futures::Stream<Item = Result<models::RepositoryName, InternalError>>
                 + Unpin
                 + Send
                 + 'static,
    > {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .select_repository_names(SelectRepositoryNamesRequest { last_index })
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into().map_ok(models::RepositoryName::from))
                .await
        }
    }

    fn merge<S>(&self, s: S) -> impl std::future::Future<Output = Result<(), InternalError>> + Send
    where
        S: futures::Stream<Item = models::RepositoryName> + Unpin + Send + 'static,
    {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .merge_repository_names(s.map(RepositoryName::from))
                .err_into()
                .map_ok(|_| ())
                .await
        }
    }
}

impl RcloneTargetPath for GRPCClient {
    async fn rclone_path(&self, transfer_id: u32) -> Result<String, InternalError> {
        let rclone_path_request = tonic::Request::new(RclonePathRequest { transfer_id });
        let RclonePathResponse { path } = self
            .client
            .write()
            .await
            .rclone_path(rclone_path_request)
            .await?
            .into_inner();
        Ok(path)
    }
}

impl Sender<models::BlobTransferItem> for GRPCClient {
    fn prepare_transfer<S>(
        &self,
        s: S,
    ) -> impl std::future::Future<Output = Result<u64, InternalError>> + Send
    where
        S: Stream<Item = models::BlobTransferItem> + Unpin + Send + 'static,
    {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            let PrepareTransferResponse { count } = guard
                .prepare_transfer(s.map(TransferItem::from))
                .await?
                .into_inner();
            Ok(count)
        }
    }
}

impl Receiver<models::BlobTransferItem> for GRPCClient {
    fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: String,
        paths: Vec<String>,
    ) -> impl std::future::Future<
        Output = impl Stream<Item = Result<models::BlobTransferItem, InternalError>>
                 + Unpin
                 + Send
                 + 'static,
    > {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .create_transfer_request(CreateTransferRequestRequest {
                    transfer_id,
                    repo_id,
                    paths,
                })
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into().map_ok(models::BlobTransferItem::from))
                .await
        }
    }

    async fn finalise_transfer(&self, transfer_id: u32) -> Result<u64, InternalError> {
        let FinaliseTransferResponse { count } = self
            .client
            .write()
            .await
            .finalise_transfer(tonic::Request::new(FinaliseTransferRequest { transfer_id }))
            .await?
            .into_inner();
        Ok(count)
    }
}
