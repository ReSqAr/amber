use crate::connection::local::LocalConfig;
use crate::connection::rclone::RCloneConfig;
use crate::connection::ssh::SshConfig;
use crate::db::models::ConnectionType;
use crate::repository::local::LocalRepository;
use crate::repository::traits::Metadata;
use crate::repository::wrapper::WrappedRepository;
use crate::utils;
use crate::utils::errors::InternalError;
use log::debug;

mod local;
mod rclone;
pub(crate) mod ssh;

#[derive(Clone, Debug)]
pub enum Config {
    Local(LocalConfig),
    Ssh(SshConfig),
    RClone(RCloneConfig),
}

impl Config {
    pub(crate) fn parse(
        connection_type: ConnectionType,
        parameter: String,
    ) -> Result<Self, InternalError> {
        match connection_type {
            ConnectionType::Local => Ok(Config::Local(LocalConfig::from_parameter(parameter)?)),
            ConnectionType::RClone => Ok(Config::RClone(RCloneConfig::from_parameter(parameter)?)),
            ConnectionType::Ssh => Ok(Config::Ssh(SshConfig::from_parameter(parameter)?)),
        }
    }

    pub(crate) async fn connect(
        &self,
        local: &LocalRepository,
        name: &str,
    ) -> Result<WrappedRepository, InternalError> {
        match self {
            Config::Local(local_config) => local_config.connect().await,
            Config::RClone(rclone_config) => rclone_config.connect(local, name).await,
            Config::Ssh(ssh_config) => ssh_config.connect().await,
        }
    }

    pub(crate) fn as_rclone_target(&self, remote_path: String) -> utils::rclone::RcloneTarget {
        match self {
            Config::Local(local_config) => local_config.as_rclone_target(remote_path),
            Config::RClone(rclone_config) => rclone_config.as_rclone_target(remote_path),
            Config::Ssh(ssh_config) => ssh_config.as_rclone_target(remote_path),
        }
    }
}

pub struct EstablishedConnection {
    #[allow(dead_code)]
    pub name: String,
    pub config: Config,
    #[allow(dead_code)]
    pub local: LocalRepository,
    pub remote: WrappedRepository,
}

impl EstablishedConnection {
    pub async fn new(
        local: LocalRepository,
        name: String,
        connection_type: ConnectionType,
        parameter: String,
    ) -> Result<Self, InternalError> {
        let config = Config::parse(connection_type, parameter)?;
        let remote = config.connect(&local, &name).await?;
        let meta = remote.current().await?;
        debug!("connected to repository via {name} to {}", meta.name);
        Ok(Self {
            local,
            name,
            config,
            remote,
        })
    }

    pub(crate) fn local_rclone_target(&self, path: String) -> utils::rclone::RcloneTarget {
        utils::rclone::RcloneTarget::Local(utils::rclone::LocalConfig { path })
    }
    pub(crate) fn remote_rclone_target(&self, remote_path: String) -> utils::rclone::RcloneTarget {
        self.config.as_rclone_target(remote_path)
    }
}
