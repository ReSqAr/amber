use crate::db::models;
use crate::flightdeck;
use crate::flightdeck::global::send;
use crate::flightdeck::observation::Message;
use crate::flightdeck::tracked::stream::Trackable;
use crate::grpc::auth::ClientAuth;
use crate::grpc::definitions::grpc_client::GrpcClient;
use crate::grpc::definitions::{
    Blob, CopiedTransferItem, CreateTransferRequestRequest, CurrentRepositoryMetadataRequest, File,
    FinaliseTransferResponse, FlightdeckMessageRequest, LookupLastIndicesRequest,
    LookupLastIndicesResponse, PrepareTransferResponse, RclonePathRequest, RclonePathResponse,
    Repository, RepositoryName, SelectBlobsRequest, SelectFilesRequest, SelectRepositoriesRequest,
    SelectRepositoryNamesRequest, TransferItem, UpdateLastIndicesRequest,
};
use crate::repository::traits::{
    LastIndices, LastIndicesSyncer, Metadata, RcloneTargetPath, Receiver, RepositoryMetadata,
    Sender, Syncer,
};
use crate::utils::errors::InternalError;
use backoff::future::retry;
use backoff::{Error as BackoffError, ExponentialBackoff};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use log::debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;

pub(crate) type ShutdownFn = Arc<std::sync::RwLock<Option<Box<dyn Fn() + Send + Sync + 'static>>>>;

#[derive(Clone)]
pub(crate) struct GRPCClient {
    client: Arc<RwLock<GrpcClient<InterceptedService<Channel, ClientAuth>>>>,
    shutdown: ShutdownFn,
}

impl Drop for GRPCClient {
    fn drop(&mut self) {
        let mut guard = self.shutdown.write().unwrap();
        if let Some(shutdown) = guard.take() {
            shutdown()
        }
    }
}

impl GRPCClient {
    fn new(
        client: GrpcClient<InterceptedService<Channel, ClientAuth>>,
        shutdown: ShutdownFn,
    ) -> Self {
        Self {
            client: Arc::new(client.into()),
            shutdown,
        }
    }

    pub async fn connect(
        addr: String,
        auth_key: String,
        shutdown: impl Fn() + Send + Sync + 'static,
    ) -> Result<Self, InternalError> {
        let attempts = Arc::new(AtomicUsize::new(0));
        let op = || {
            let attempts = attempts.clone();
            let addr = addr.clone();
            let auth_key = auth_key.clone();

            async move {
                let current_attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                debug!("Attempt {}: connecting to {}", current_attempt, addr);
                if current_attempt > 3 {
                    return Err(BackoffError::permanent(InternalError::Grpc(
                        "maximum retry attempts reached".into(),
                    )));
                }

                let channel = tonic::transport::Endpoint::from_shared(addr.clone())
                    .map_err(InternalError::Tonic)
                    .map_err(BackoffError::transient)?
                    .connect()
                    .await
                    .map_err(InternalError::Tonic)
                    .map_err(BackoffError::transient)?;

                let interceptor = ClientAuth::new(&auth_key)
                    .map_err(|e| {
                        InternalError::Grpc(format!("unable to create authentication method: {e}"))
                    })
                    .map_err(BackoffError::transient)?;
                let client = GrpcClient::with_interceptor(channel, interceptor);

                debug!("connected to {}", &addr);
                Ok(client)
            }
        };

        let backoff = ExponentialBackoff::default();
        let mut client = retry(backoff, op).await?;
        debug!(
            "Successfully connected after {} attempt(s)",
            attempts.load(Ordering::SeqCst)
        );

        let flightdeck_handle = Self::forward_flightdeck_messages(&mut client).await?;

        let shutdown: Box<dyn Fn() + Send + Sync + 'static> = Box::new(move || {
            shutdown();
            flightdeck_handle.abort();
        });
        let shutdown = Arc::new(std::sync::RwLock::new(Some(shutdown)));

        Ok(Self::new(client, shutdown.clone()))
    }

    async fn forward_flightdeck_messages(
        client: &mut GrpcClient<InterceptedService<Channel, ClientAuth>>,
    ) -> Result<JoinHandle<()>, InternalError> {
        let flightdesk_request = tonic::Request::new(FlightdeckMessageRequest {});
        let mut stream = client
            .flightdeck_messages(flightdesk_request)
            .await?
            .into_inner()
            .map_ok(flightdeck::observation::Message::from);

        let forward_handle = tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(Message { level, observation }) => send(level, observation),
                    Err(e) => log::error!("failed to forward flightdeck observation: {e}"),
                };
            }
        });

        Ok(forward_handle)
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
    ) -> impl Future<
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
                .track("GRPCClient::Syncer<models::Repository>::select")
        }
    }

    fn merge<S>(&self, s: S) -> impl Future<Output = Result<(), InternalError>> + Send
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
    ) -> impl Future<
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
                .track("GRPCClient::Syncer<models::File>::select")
        }
    }

    fn merge<S>(&self, s: S) -> impl Future<Output = Result<(), InternalError>> + Send
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
    ) -> impl Future<
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
                .track("GRPCClient::Syncer<models::Blob>::select")
        }
    }

    fn merge<S>(&self, s: S) -> impl Future<Output = Result<(), InternalError>> + Send
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
    ) -> impl Future<
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
                .track("GRPCClient::Syncer<models::RepositoryName>::select")
        }
    }

    fn merge<S>(&self, s: S) -> impl Future<Output = Result<(), InternalError>> + Send
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
    fn prepare_transfer<S>(&self, s: S) -> impl Future<Output = Result<u64, InternalError>> + Send
    where
        S: futures::Stream<Item = models::BlobTransferItem> + Unpin + Send + 'static,
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
    ) -> impl Future<
        Output = impl futures::Stream<Item = Result<models::BlobTransferItem, InternalError>>
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
                .track("GRPCClient::Receiver<models::BlobTransferItem>>::create_transfer_request")
        }
    }

    fn finalise_transfer(
        &self,
        s: impl Stream<Item = models::CopiedTransferItem> + Unpin + Send + 'static,
    ) -> impl Future<Output = Result<u64, InternalError>> + Send {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            let FinaliseTransferResponse { count } = guard
                .finalise_transfer(s.map(CopiedTransferItem::from))
                .await?
                .into_inner();
            Ok(count)
        }
    }
}
