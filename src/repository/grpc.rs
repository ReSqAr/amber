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
    RepositoryMetadata, RepositorySyncState, SelectBlobsRequest, SelectFilesRequest,
    SelectRepositoryMetadataRequest, SelectRepositorySyncStatesRequest, TransferItem,
    UpdateLastIndicesRequest,
};
use crate::repository::traits::{
    LastSyncState, LastSyncStateSyncer, Metadata, RcloneTargetPath, Receiver,
    RepositoryCurrentMetadata, Sender, Syncer,
};
use crate::utils::errors::InternalError;
use backoff::future::retry;
use backoff::{Error as BackoffError, ExponentialBackoff};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt, stream};
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

                debug!("connected to {}", addr);
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
                    .and_then(Message::try_from)
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

/// Turns the result of a server-streaming call into a stream.
///
/// The call itself can fail (transport, authentication, a server-side error
/// raised before the first message); that failure has to be reported through the
/// stream, because the `Syncer`/`Receiver` traits hand back a bare stream.
fn response_stream<P, T>(
    response: Result<tonic::Response<tonic::Streaming<P>>, tonic::Status>,
    name: &'static str,
    convert: fn(P) -> T,
) -> BoxStream<'static, Result<T, InternalError>>
where
    P: Send + Sync + 'static,
    T: Send + Sync + 'static,
{
    match response {
        Ok(response) => response
            .into_inner()
            .map_ok(convert)
            .err_into()
            .boxed()
            .track(name)
            .boxed(),
        Err(status) => {
            log::error!("{name} failed: {status}");
            stream::iter([Err(InternalError::Status(status))]).boxed()
        }
    }
}

impl Metadata for GRPCClient {
    fn current(&self) -> BoxFuture<'_, Result<RepositoryCurrentMetadata, InternalError>> {
        let client = self.client.clone();
        Box::pin(async move {
            let repo_id_request = tonic::Request::new(CurrentRepositoryMetadataRequest {});
            let meta = client
                .write()
                .await
                .current_repository_metadata(repo_id_request)
                .await?
                .into_inner();
            Ok(RepositoryCurrentMetadata {
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
            let response = {
                let mut guard = arc_client.write().await;
                guard
                    .select_repository_sync_states(SelectRepositorySyncStatesRequest {})
                    .await
            };
            response_stream(
                response,
                "GRPCClient::Syncer<models::Repository>::select",
                models::RepositorySyncState::from,
            )
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
            let response = {
                let mut guard = arc_client.write().await;
                guard.select_files(SelectFilesRequest { last_index }).await
            };
            response_stream(
                response,
                "GRPCClient::Syncer<models::File>::select",
                models::File::from,
            )
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
            let response = {
                let mut guard = arc_client.write().await;
                guard.select_blobs(SelectBlobsRequest { last_index }).await
            };
            response_stream(
                response,
                "GRPCClient::Syncer<models::Blob>::select",
                models::Blob::from,
            )
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

impl Syncer<models::RepositoryMetadata> for GRPCClient {
    fn select(
        &self,
        last_index: Option<u64>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<models::RepositoryMetadata, InternalError>>> {
        let arc_client = self.client.clone();
        async move {
            let response = {
                let mut guard = arc_client.write().await;
                guard
                    .select_repository_metadata(SelectRepositoryMetadataRequest { last_index })
                    .await
            };
            response_stream(
                response,
                "GRPCClient::Syncer<models::RepositoryMetadata>::select",
                models::RepositoryMetadata::from,
            )
        }
        .boxed()
    }

    fn merge(
        &self,
        s: BoxStream<'static, models::RepositoryMetadata>,
    ) -> BoxFuture<'_, Result<(), InternalError>> {
        let arc_client = self.client.clone();
        async move {
            let mut guard = arc_client.write().await;
            guard
                .merge_repository_metadata(s.map(RepositoryMetadata::from).boxed())
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
            let response = {
                let mut guard = arc_client.write().await;
                guard
                    .create_transfer_request(CreateTransferRequestRequest {
                        transfer_id,
                        repo_id: repo_id.0,
                        paths,
                    })
                    .await
            };
            response_stream(
                response,
                "GRPCClient::Receiver<models::BlobTransferItem>>::create_transfer_request",
                models::BlobTransferItem::from,
            )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::serve;
    use crate::flightdeck::output::Output;
    use crate::repository::local::{LocalRepository, LocalRepositoryConfig};
    use crate::utils::port::find_available_port;
    use futures::TryStreamExt;
    use std::io::Write;
    use tempfile::TempDir;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;

    struct Sink;
    impl Write for Sink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn sink() -> Output {
        Output::Override(Arc::new(std::sync::Mutex::new(
            Box::new(Sink) as Box<dyn Write + Send + Sync>
        )))
    }

    /// Serves a fresh repository on a free port; returns the address, the auth key
    /// it expects, a handle to stop it and the join handle of the server task.
    async fn serve_repository(
        dir: &TempDir,
    ) -> (String, String, oneshot::Sender<()>, JoinHandle<()>) {
        let root = dir.path().join("repo");
        tokio::fs::create_dir_all(&root).await.expect("create root");
        LocalRepository::create(
            LocalRepositoryConfig {
                maybe_root: Some(root.clone()),
                app_folder: ".amb".into(),
                preferred_capability: None,
            },
            "served".into(),
        )
        .await
        .expect("create repository")
        .close()
        .await
        .expect("close repository");

        let port = find_available_port().await.expect("free port");
        let auth_key = serve::generate_auth_key();
        let (stop_tx, stop_rx) = oneshot::channel();

        let auth_key_clone = auth_key.clone();
        let handle = tokio::spawn(async move {
            let shutdown = async {
                let _ = stop_rx.await;
            }
            .boxed();
            if let Err(e) = serve::serve_on_port(
                Some(root),
                ".amb".into(),
                None,
                sink(),
                port,
                auth_key_clone,
                shutdown,
            )
            .await
            {
                log::error!("test server stopped with an error: {e}");
            }
        });

        (
            format!("http://127.0.0.1:{port}"),
            auth_key,
            stop_tx,
            handle,
        )
    }

    /// A connected client keeps a flightdeck stream open for its whole lifetime,
    /// and a graceful shutdown waits for exactly that - so stop the server hard.
    async fn stop_server(stop_tx: oneshot::Sender<()>, handle: JoinHandle<()>) {
        let _ = stop_tx.send(());
        handle.abort();
        let _ = handle.await;
    }

    /// A rejected authentication token has to surface as an error - the very first
    /// thing a client does is an RPC, so this is the failed-call path.
    #[tokio::test(flavor = "multi_thread")]
    async fn connecting_with_a_wrong_auth_key_is_an_error() {
        let dir = TempDir::new().expect("tempdir");
        let (addr, _auth_key, stop_tx, handle) = serve_repository(&dir).await;

        let client = GRPCClient::connect(addr, "not-the-auth-key".to_string(), || {}).await;
        assert!(
            client.is_err(),
            "expected an authentication error, got a connected client"
        );

        stop_server(stop_tx, handle).await;
    }

    /// The `Syncer`/`Receiver` traits hand back a bare stream, so a failure of the
    /// streaming call itself has to be reported as an item of that stream rather
    /// than taking the process down.
    #[tokio::test(flavor = "multi_thread")]
    async fn select_reports_transport_errors_through_the_stream() {
        let dir = TempDir::new().expect("tempdir");
        let (addr, auth_key, stop_tx, handle) = serve_repository(&dir).await;

        let client = GRPCClient::connect(addr, auth_key, || {})
            .await
            .expect("connect");

        // The server goes away underneath a connected client.
        stop_server(stop_tx, handle).await;

        let files: Result<Vec<models::File>, _> = Syncer::<models::File>::select(&client, None)
            .await
            .try_collect()
            .await;
        assert!(
            files.is_err(),
            "expected the failed call to be reported as a stream error"
        );

        let blobs: Result<Vec<models::Blob>, _> = Syncer::<models::Blob>::select(&client, None)
            .await
            .try_collect()
            .await;
        assert!(blobs.is_err(), "expected a stream error for blobs as well");
    }
}
