use crate::db::models::ConnectionType;
use crate::repository::local::LocalRepository;
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::InternalError;
use crate::utils::rclone;

#[derive(Clone, Debug)]
pub struct LocalConfig {
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

pub fn parse_config(
    connection_type: ConnectionType,
    parameter: String,
) -> Result<ConnectionConfig, InternalError> {
    match connection_type {
        ConnectionType::Local => Ok(ConnectionConfig::Local(LocalConfig { root: parameter })),
    }
}

pub async fn connect(config: &ConnectionConfig) -> Result<WrappedRepository, InternalError> {
    match config.clone() {
        ConnectionConfig::Local(local_config) => {
            let LocalConfig { root } = local_config;
            let repository = LocalRepository::new(Some(root.clone().into())).await?;
            Ok(WrappedRepository::Local(repository))
        }
    }
}
