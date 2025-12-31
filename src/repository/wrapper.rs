use crate::repository::grpc::GRPCClient;
use crate::repository::local::LocalRepository;
use crate::repository::rclone::RCloneStore;
use crate::repository::traits::{Metadata, RepositoryCurrentMetadata};
use crate::utils::errors::InternalError;
use futures_core::future::BoxFuture;

#[derive(Clone)]
pub enum WrappedRepository {
    Local(LocalRepository),
    Grpc(GRPCClient),
    RClone(RCloneStore),
}

impl WrappedRepository {
    pub(crate) async fn close(&self) -> Result<(), InternalError> {
        match self {
            WrappedRepository::Local(local) => local.close().await,
            WrappedRepository::Grpc(grpc) => grpc.close().await,
            WrappedRepository::RClone(rclone) => rclone.close().await,
        }
    }
}

impl Metadata for WrappedRepository {
    fn current(&self) -> BoxFuture<'_, Result<RepositoryCurrentMetadata, InternalError>> {
        Box::pin(async move {
            match self {
                WrappedRepository::Local(local) => local.current().await,
                WrappedRepository::Grpc(grpc) => grpc.current().await,
                WrappedRepository::RClone(rclone) => rclone.current().await,
            }
        })
    }
}
