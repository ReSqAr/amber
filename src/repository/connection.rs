use crate::db::models::ConnectionType;
use crate::repository::grpc::GRPCClient;
use crate::repository::local_repository::LocalRepository;
use crate::repository::traits::Metadata;
use log::debug;

pub enum ConnectedRepository {
    Local(LocalRepository),
    Grpc(GRPCClient),
}

pub struct Connection {
    pub repository: ConnectedRepository,
}

impl Connection {
    pub async fn connect(
        name: String,
        connection_type: ConnectionType,
        parameter: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match connection_type {
            ConnectionType::Local => {
                let root = parameter;
                let repository = LocalRepository::new(Some(root.parse()?)).await?;
                debug!(
                    "connecting to local database {} at {}: {}",
                    name,
                    root,
                    repository.repo_id().await?
                );
                Ok(Self {
                    repository: ConnectedRepository::Local(repository),
                })
            }
        }
    }
}
