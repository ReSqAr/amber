use crate::db::models::ConnectionType;
use crate::repository::grpc::GRPCClient;
use crate::repository::local_repository::LocalRepository;
use crate::repository::traits::ConnectionManager;
use anyhow::anyhow;

pub enum ConnectedRepository {
    Local(LocalRepository),
    Grpc(GRPCClient),
}

pub struct Connection {
    repository: ConnectedRepository,
}

pub async fn connect(
    manager: impl ConnectionManager,
    name: String,
) -> Result<Connection, Box<dyn std::error::Error>> {
    if let Ok(Some(connection)) = manager.by_name(&name).await {
        Connection::connect(connection).await
    } else {
        Err(anyhow!("unable to find the connection '{}'", name).into())
    }
}

impl Connection {
    pub async fn connect(
        connection: crate::db::models::Connection,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match connection.connection_type {
            ConnectionType::Local => {
                let root = connection.parameter;
                Ok(Self {
                    repository: ConnectedRepository::Local(
                        LocalRepository::new(Some(root.parse()?)).await?,
                    ),
                })
            }
        }
    }
}
