use crate::db::models::TransferItem;
use crate::repository::grpc::GRPCClient;
use crate::repository::local::LocalRepository;
use crate::repository::rclone::RCloneClient;
use crate::repository::traits::{
    BlobReceiver, BlobSender, LastIndices, LastIndicesSyncer, Metadata, Syncer, SyncerParams,
};
use crate::utils::errors::InternalError;
use futures::{Stream, StreamExt};

#[derive(Clone)]
pub enum Repository {
    Local(LocalRepository),
    Grpc(GRPCClient),
    RCloneExporter(RCloneClient),
}

impl Metadata for Repository {
    async fn repo_id(&self) -> Result<String, InternalError> {
        match self {
            Repository::Local(local) => local.repo_id().await,
            Repository::Grpc(grpc) => grpc.repo_id().await,
            Repository::RCloneExporter(rclone) => rclone.repo_id().await,
        }
    }
}

impl Repository {
    pub(crate) fn as_managed(&self) -> Option<ManagedRepository> {
        match self {
            Repository::Local(local) => Some(ManagedRepository::Local(local.clone())),
            Repository::Grpc(grpc) => Some(ManagedRepository::Grpc(grpc.clone())),
            Repository::RCloneExporter(_) => None,
        }
    }
}

#[derive(Clone)]
pub enum ManagedRepository {
    Local(LocalRepository),
    Grpc(GRPCClient),
}

impl Metadata for ManagedRepository {
    async fn repo_id(&self) -> Result<String, InternalError> {
        match self {
            ManagedRepository::Local(local) => local.repo_id().await,
            ManagedRepository::Grpc(grpc) => grpc.repo_id().await,
        }
    }
}

impl LastIndicesSyncer for ManagedRepository {
    async fn lookup(&self, repo_id: String) -> Result<LastIndices, InternalError> {
        match self {
            ManagedRepository::Local(local) => local.lookup(repo_id).await,
            ManagedRepository::Grpc(grpc) => grpc.lookup(repo_id).await,
        }
    }

    async fn refresh(&self) -> Result<(), InternalError> {
        match self {
            ManagedRepository::Local(local) => local.refresh().await,
            ManagedRepository::Grpc(grpc) => grpc.refresh().await,
        }
    }
}

impl<T: SyncerParams + 'static> Syncer<T> for ManagedRepository
where
    LocalRepository: Syncer<T>,
    GRPCClient: Syncer<T>,
    <T as SyncerParams>::Params: Send,
{
    async fn select(
        &self,
        params: <T as SyncerParams>::Params,
    ) -> impl Stream<Item = Result<T, InternalError>> + Unpin + Send + 'static {
        match self {
            ManagedRepository::Local(local) => {
                <LocalRepository as Syncer<T>>::select(local, params)
                    .await
                    .boxed()
            }
            ManagedRepository::Grpc(grpc) => <GRPCClient as Syncer<T>>::select(grpc, params)
                .await
                .boxed(),
        }
    }

    async fn merge<S>(&self, s: S) -> Result<(), InternalError>
    where
        S: Stream<Item = T> + Unpin + Send + 'static,
    {
        match self {
            ManagedRepository::Local(local) => local.merge(s).await,
            ManagedRepository::Grpc(grpc) => grpc.merge(s).await,
        }
    }
}

impl BlobSender for ManagedRepository {
    async fn prepare_transfer<S>(&self, s: S) -> Result<(), InternalError>
    where
        S: Stream<Item = TransferItem> + Unpin + Send + 'static,
    {
        match self {
            ManagedRepository::Local(local) => local.prepare_transfer(s).await,
            ManagedRepository::Grpc(grpc) => grpc.prepare_transfer(s).await,
        }
    }
}

impl BlobReceiver for ManagedRepository {
    async fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: String,
    ) -> impl Stream<Item = Result<TransferItem, InternalError>> + Unpin + Send + 'static {
        match self {
            ManagedRepository::Local(local) => local
                .create_transfer_request(transfer_id, repo_id)
                .await
                .boxed(),
            ManagedRepository::Grpc(grpc) => grpc
                .create_transfer_request(transfer_id, repo_id)
                .await
                .boxed(),
        }
    }

    async fn finalise_transfer(&self, transfer_id: u32) -> Result<(), InternalError> {
        match self {
            ManagedRepository::Local(local) => local.finalise_transfer(transfer_id).await,
            ManagedRepository::Grpc(grpc) => grpc.finalise_transfer(transfer_id).await,
        }
    }
}
