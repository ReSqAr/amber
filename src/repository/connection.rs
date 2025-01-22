use crate::db::models::ConnectionType;
use crate::repository::grpc::GRPCClient;
use crate::repository::local::LocalRepository;
use crate::repository::rclone::RCloneClient;
use crate::repository::traits::{LastIndices, LastIndicesSyncer, Metadata, Syncer, SyncerParams};
use crate::utils::app_error::AppError;
use futures::{Stream, StreamExt};
use log::debug;

#[derive(Clone)]
pub enum Repository {
    Local(LocalRepository),
    Grpc(GRPCClient),
    RCloneExporter(RCloneClient),
}

impl Repository {
    pub(crate) fn as_tracked(&self) -> Option<TrackingRepository> {
        match self {
            Repository::Local(local) => Some(TrackingRepository::Local(local.clone())),
            Repository::Grpc(grpc) => Some(TrackingRepository::Grpc(grpc.clone())),
            Repository::RCloneExporter(_) => None,
        }
    }
}

impl Metadata for Repository {
    async fn repo_id(&self) -> Result<String, AppError> {
        match self {
            Repository::Local(local) => local.repo_id().await,
            Repository::Grpc(grpc) => grpc.repo_id().await,
            Repository::RCloneExporter(rclone) => rclone.repo_id().await,
        }
    }
}

#[derive(Clone)]
pub enum TrackingRepository {
    Local(LocalRepository),
    Grpc(GRPCClient),
}

impl Metadata for TrackingRepository {
    async fn repo_id(&self) -> Result<String, AppError> {
        match self {
            TrackingRepository::Local(local) => local.repo_id().await,
            TrackingRepository::Grpc(grpc) => grpc.repo_id().await,
        }
    }
}

impl LastIndicesSyncer for TrackingRepository {
    async fn lookup(&self, repo_id: String) -> Result<LastIndices, AppError> {
        match self {
            TrackingRepository::Local(local) => local.lookup(repo_id).await,
            TrackingRepository::Grpc(grpc) => grpc.lookup(repo_id).await,
        }
    }

    async fn refresh(&self) -> Result<(), AppError> {
        match self {
            TrackingRepository::Local(local) => local.refresh().await,
            TrackingRepository::Grpc(grpc) => grpc.refresh().await,
        }
    }
}

impl<T: SyncerParams + 'static> Syncer<T> for TrackingRepository
where
    LocalRepository: Syncer<T>,
    GRPCClient: Syncer<T>,
    <T as SyncerParams>::Params: Send,
{
    async fn select(
        &self,
        params: <T as SyncerParams>::Params,
    ) -> impl Stream<Item = Result<T, AppError>> + Unpin + Send + 'static {
        match self {
            TrackingRepository::Local(local) => {
                <LocalRepository as Syncer<T>>::select(local, params)
                    .await
                    .boxed()
            }
            TrackingRepository::Grpc(grpc) => <GRPCClient as Syncer<T>>::select(grpc, params)
                .await
                .boxed(),
        }
    }

    async fn merge<S>(&self, s: S) -> Result<(), AppError>
    where
        S: Stream<Item = T> + Unpin + Send + 'static,
    {
        match self {
            TrackingRepository::Local(local) => local.merge(s).await,
            TrackingRepository::Grpc(grpc) => grpc.merge(s).await,
        }
    }
}

pub struct ConnectedRepository {
    pub repository: Repository,
}

impl ConnectedRepository {
    pub async fn connect(
        name: String,
        connection_type: ConnectionType,
        parameter: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match connection_type {
            ConnectionType::Local => {
                let root = parameter;
                let repository = LocalRepository::new(Some(root.parse()?)).await?;
                let repo_id = repository.repo_id().await?;
                debug!(
                    "connecting to local database {} at {}: {}",
                    name, root, repo_id
                );
                Ok(Self {
                    repository: Repository::Local(repository),
                })
            }
        }
    }
}
