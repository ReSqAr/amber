use crate::db::models::ConnectionType;
use crate::repository::local::LocalRepository;
use crate::repository::repository::Repository;
use crate::repository::traits::Metadata;
use crate::utils::errors::InternalError;
use crate::utils::rclone;
use log::debug;

#[derive(Clone, Debug)]
struct LocalConfig {
    root: String,
}

impl LocalConfig {
    pub(crate) fn as_rclone_target(&self) -> rclone::RcloneTarget {
        rclone::RcloneTarget::Local(rclone::LocalConfig {
            path: self.root.clone().into(),
        })
    }
}

#[derive(Clone, Debug)]
pub enum ConnectionConfig {
    Local(LocalConfig),
}

impl ConnectionConfig {
    pub(crate) fn as_rclone_target(&self) -> rclone::RcloneTarget {
        match self {
            ConnectionConfig::Local(local_config) => local_config.as_rclone_target(),
        }
    }
}

fn parse_config(
    connection_type: ConnectionType,
    parameter: String,
) -> Result<ConnectionConfig, InternalError> {
    match connection_type {
        ConnectionType::Local => Ok(ConnectionConfig::Local(LocalConfig { root: parameter })),
    }
}

pub struct EstablishedConnection {
    pub name: String,
    pub config: ConnectionConfig,
    pub local: LocalRepository,
    pub remote: Repository,
}

impl EstablishedConnection {
    pub async fn connect(
        local: LocalRepository,
        name: String,
        connection_type: ConnectionType,
        parameter: String,
    ) -> Result<Self, InternalError> {
        let config = parse_config(connection_type, parameter)?;
        match config.clone() {
            ConnectionConfig::Local(LocalConfig { root }) => {
                let repository = LocalRepository::new(Some(root.clone().into())).await?;
                let repo_id = repository.repo_id().await?;
                debug!("connected to local database {name} at {root}: {repo_id}");
                Ok(Self {
                    local,
                    name,
                    config,
                    remote: Repository::Local(repository),
                })
            }
        }
    }

    pub(crate) fn remote_rclone_target(&self) -> rclone::RcloneTarget {
        self.config.as_rclone_target()
    }
}
