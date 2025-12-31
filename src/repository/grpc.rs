use crate::db::models;
use crate::db::models::RepoID;
use crate::flightdeck::global::send;
use crate::flightdeck::observation::Message;
use crate::flightdeck::tracked::stream::Trackable;
use crate::grpc::auth::ClientAuth;
use crate::grpc::definitions::grpc_client::GrpcClient;
use crate::grpc::definitions::{
    Blob, CopiedTransferItem, CreateTransferRequestRequest, CurrentRepositoryMetadataRequest, File,
    FinaliseTransferResponse, FlightdeckMessageRequest, LookupLastIndicesRequest,
    LookupLastIndicesResponse, PrepareTransferResponse, RclonePathRequest, RclonePathResponse,
    RepositoryName, RepositorySyncState, SelectBlobsRequest, SelectFilesRequest,
    SelectRepositoryNamesRequest, SelectRepositorySyncStatesRequest, TransferItem,
    UpdateLastIndicesRequest,
};
use crate::repository::traits::{
    LastSyncState, LastSyncStateSyncer, Metadata, RcloneTargetPath, Receiver, RepositoryMetadata,
    Sender, Syncer,
};
use crate::utils::errors::InternalError;
use backoff::future::retry;
use backoff::{Error as BackoffError, ExponentialBackoff};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
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

impl GRPCClient {
    pub(crate) async fn close(&self) -> Result<(), InternalError> {
        let mut guard = self.shutdown.write().unwrap();
        if let Some(shutdown) = guard.take() {
            shutdown()
        };
        Ok(())
    }
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
            .map(|m| {
                m.map_err(Into::<InternalError>::into)
                    .and_then(|m| Message::try_from(m).map_err(Into::into))
            });

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
    fn current(&self) -> BoxFuture<'_, Result<RepositoryMetadata, InternalError>> {
        let client = self.client.clone();
        Box::pin(async move {
            let repo_id_request = tonic::Request::new(CurrentRepositoryMetadataRequest {});
            let meta = client
                .write()
                .await
                .current_repository_metadata(repo_id_request)
                .await?
                .into_inner();
            Ok(RepositoryMetadata {
                id: RepoID(meta.id),
                name: meta.name,
            })
        })
    }
}

impl LastSyncStateSyncer for GRPCClient {
    fn lookup(&self, repo_id: RepoID) -> BoxFuture<'_, Result<LastSyncState, InternalError>> {
        let client = self.client.clone();
        async move {
            let LookupLastIndicesResponse { file, blob, name } = client
                .write()
                .await
                .lookup_last_indices(LookupLastIndicesRequest {
                    repo_id: repo_id.0.clone(),
                })
                .await?
                .into_inner();
            Ok(LastSyncState { file, blob, name })
        }
        .boxed()
    }

    fn refresh(&self) -> BoxFuture<'_, Result<(), InternalError>> {
        let client = self.client.clone();
        async move {
            client
                .write()
                .await
                .update_last_indices(UpdateLastIndicesRequest {})
                .await?;
            Ok(())
        }
        .boxed()
    }
}

impl Syncer<models::RepositorySyncState> for GRPCClient {
    fn select(
        &self,
        _params: (),
    ) -> BoxFuture<'_, BoxStream<'static, Result<models::RepositorySyncState, InternalError>>> {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .select_repository_sync_states(SelectRepositorySyncStatesRequest {})
                .map_ok(tonic::Response::into_inner)
                .map(|r| {
                    r.unwrap()
                        .err_into()
                        .map_ok(models::RepositorySyncState::from)
                })
                .await
                .track("GRPCClient::Syncer<models::Repository>::select")
                .boxed()
        }
        .boxed()
    }

    fn merge(
        &self,
        s: BoxStream<'static, models::RepositorySyncState>,
    ) -> BoxFuture<'_, Result<(), InternalError>> {
        let arc_client = self.client.clone();
        Box::pin(async move {
            let mut guard = arc_client.write().await;
            guard
                .merge_repository_sync_states(s.map(RepositorySyncState::from).boxed())
                .err_into()
                .map_ok(|_| ())
                .boxed()
                .await
        })
    }
}

impl Syncer<models::File> for GRPCClient {
    fn select(
        &self,
        last_index: Option<u64>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<models::File, InternalError>>> {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .select_files(SelectFilesRequest { last_index })
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into().map_ok(models::File::from))
                .await
                .track("GRPCClient::Syncer<models::File>::select")
                .boxed()
        }
        .boxed()
    }

    fn merge(
        &self,
        s: BoxStream<'static, models::File>,
    ) -> BoxFuture<'_, Result<(), InternalError>> {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .merge_files(s.map(File::from).boxed())
                .err_into()
                .map_ok(|_| ())
                .boxed()
                .await
        }
        .boxed()
    }
}

impl Syncer<models::Blob> for GRPCClient {
    fn select(
        &self,
        last_index: Option<u64>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<models::Blob, InternalError>>> {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .select_blobs(SelectBlobsRequest { last_index })
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into().map_ok(models::Blob::from))
                .await
                .track("GRPCClient::Syncer<models::Blob>::select")
                .boxed()
        }
        .boxed()
    }

    fn merge(
        &self,
        s: BoxStream<'static, models::Blob>,
    ) -> BoxFuture<'_, Result<(), InternalError>> {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .merge_blobs(s.map(Blob::from).boxed())
                .err_into()
                .map_ok(|_| ())
                .boxed()
                .await
        }
        .boxed()
    }
}

impl Syncer<models::RepositoryName> for GRPCClient {
    fn select(
        &self,
        last_index: Option<u64>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<models::RepositoryName, InternalError>>> {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .select_repository_names(SelectRepositoryNamesRequest { last_index })
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into().map_ok(models::RepositoryName::from))
                .await
                .track("GRPCClient::Syncer<models::RepositoryName>::select")
                .boxed()
        }
        .boxed()
    }

    fn merge(
        &self,
        s: BoxStream<'static, models::RepositoryName>,
    ) -> BoxFuture<'_, Result<(), InternalError>> {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .merge_repository_names(s.map(RepositoryName::from).boxed())
                .err_into()
                .map_ok(|_| ())
                .boxed()
                .await
        }
        .boxed()
    }
}

impl RcloneTargetPath for GRPCClient {
    fn rclone_path(&self, transfer_id: u32) -> BoxFuture<'_, Result<String, InternalError>> {
        let client = self.client.clone();
        async move {
            let rclone_path_request = tonic::Request::new(RclonePathRequest { transfer_id });
            let mut guard = client.write().boxed().await;
            let RclonePathResponse { path } = guard
                .rclone_path(rclone_path_request)
                .boxed()
                .await?
                .into_inner();
            Ok(path)
        }
        .boxed()
    }
}

impl Sender<models::BlobTransferItem> for GRPCClient {
    fn prepare_transfer(
        &self,
        s: BoxStream<'static, models::BlobTransferItem>,
    ) -> BoxFuture<'_, Result<u64, InternalError>> {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            let PrepareTransferResponse { count } = guard
                .prepare_transfer(s.map(TransferItem::from).boxed())
                .boxed()
                .await?
                .into_inner();
            Ok(count)
        }
        .boxed()
    }
}

impl Receiver<models::BlobTransferItem> for GRPCClient {
    fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: RepoID,
        paths: Vec<String>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<models::BlobTransferItem, InternalError>>> {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .create_transfer_request(CreateTransferRequestRequest {
                    transfer_id,
                    repo_id: repo_id.0,
                    paths,
                })
                .map_ok(tonic::Response::into_inner)
                .map(|r| r.unwrap().err_into().map_ok(models::BlobTransferItem::from))
                .await
                .track("GRPCClient::Receiver<models::BlobTransferItem>>::create_transfer_request")
                .boxed()
        }
        .boxed()
    }

    fn finalise_transfer(
        &self,
        s: BoxStream<'static, models::CopiedTransferItem>,
    ) -> BoxFuture<'_, Result<u64, InternalError>> {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            let FinaliseTransferResponse { count } = guard
                .finalise_transfer(s.map(CopiedTransferItem::from).boxed())
                .boxed()
                .await?
                .into_inner();
            Ok(count)
        }
        .boxed()
    }
}
