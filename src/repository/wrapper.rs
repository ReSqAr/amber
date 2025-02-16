use crate::db::models::BlobTransferItem;
use crate::repository::grpc::GRPCClient;
use crate::repository::local::LocalRepository;
use crate::repository::rclone::RCloneStore;
use crate::repository::traits::{
    LastIndices, LastIndicesSyncer, Metadata, RcloneTargetPath, Receiver, RepositoryMetadata,
    Sender, Syncer, SyncerParams,
};
use crate::utils::errors::InternalError;
use futures::{Stream, StreamExt};

pub enum WrappedRepository {
    Local(LocalRepository),
    Grpc(GRPCClient),
    RClone(RCloneStore),
}

impl Metadata for WrappedRepository {
    async fn current(&self) -> Result<RepositoryMetadata, InternalError> {
        match self {
            WrappedRepository::Local(local) => local.current().await,
            WrappedRepository::Grpc(grpc) => grpc.current().await,
            WrappedRepository::RClone(rclone) => rclone.current().await,
        }
    }
}

impl WrappedRepository {
    pub(crate) fn as_managed(&self) -> Option<ManagedRepository> {
        match self {
            WrappedRepository::Local(local) => Some(ManagedRepository::Local(local.clone())),
            WrappedRepository::Grpc(grpc) => Some(ManagedRepository::Grpc(grpc.clone())),
            WrappedRepository::RClone(_) => None,
        }
    }
}

#[derive(Clone)]
pub enum ManagedRepository {
    Local(LocalRepository),
    Grpc(GRPCClient),
}

impl Metadata for ManagedRepository {
    async fn current(&self) -> Result<RepositoryMetadata, InternalError> {
        match self {
            ManagedRepository::Local(local) => local.current().await,
            ManagedRepository::Grpc(grpc) => grpc.current().await,
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

impl Sender<BlobTransferItem> for ManagedRepository {
    async fn prepare_transfer<S>(&self, s: S) -> Result<u64, InternalError>
    where
        S: Stream<Item = BlobTransferItem> + Unpin + Send + 'static,
    {
        match self {
            ManagedRepository::Local(local) => {
                Sender::<BlobTransferItem>::prepare_transfer(local, s).await
            }
            ManagedRepository::Grpc(grpc) => grpc.prepare_transfer(s).await,
        }
    }
}

impl RcloneTargetPath for ManagedRepository {
    async fn rclone_path(&self, transfer_id: u32) -> Result<String, InternalError> {
        match self {
            ManagedRepository::Local(local) => local.rclone_path(transfer_id).await,
            ManagedRepository::Grpc(grpc) => grpc.rclone_path(transfer_id).await,
        }
    }
}

impl Receiver<BlobTransferItem> for ManagedRepository {
    async fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: String,
    ) -> impl Stream<Item = Result<BlobTransferItem, InternalError>> + Unpin + Send + 'static {
        match self {
            ManagedRepository::Local(local) => {
                Receiver::<BlobTransferItem>::create_transfer_request(local, transfer_id, repo_id)
                    .await
                    .boxed()
            }
            ManagedRepository::Grpc(grpc) => grpc
                .create_transfer_request(transfer_id, repo_id)
                .await
                .boxed(),
        }
    }

    async fn finalise_transfer(&self, transfer_id: u32) -> Result<u64, InternalError> {
        match self {
            ManagedRepository::Local(local) => {
                Receiver::<BlobTransferItem>::finalise_transfer(local, transfer_id).await
            }
            ManagedRepository::Grpc(grpc) => grpc.finalise_transfer(transfer_id).await,
        }
    }
}
