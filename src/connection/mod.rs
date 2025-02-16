use crate::connection::local::LocalConfig;
use crate::connection::rclone::RCloneConfig;
use crate::connection::ssh::SshConfig;
use crate::db::models::ConnectionType;
use crate::repository::local::LocalRepository;
use crate::repository::traits::Metadata;
use crate::repository::wrapper::{ManagedRepository, WrappedRepository};
use crate::utils;
use crate::utils::errors::{AppError, InternalError};
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

    async fn connect(&self) -> Result<WrappedRepository, InternalError> {
        match self {
            Config::Local(local_config) => local_config.connect().await,
            Config::RClone(rclone_config) => rclone_config.connect().await,
            Config::Ssh(ssh_config) => ssh_config.connect().await,
        }
    }

    pub(crate) fn as_rclone_target(&self) -> utils::rclone::RcloneTarget {
        match self {
            Config::Local(local_config) => local_config.as_rclone_target(),
            Config::RClone(rclone_config) => rclone_config.as_rclone_target(),
            Config::Ssh(ssh_config) => ssh_config.as_rclone_target(),
        }
    }
}

pub struct EstablishedConnection {
    pub name: String,
    pub config: Config,
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
        let remote = config.connect().await?;
        let meta = remote.current().await?;
        debug!("connected to repository via {name} to {}", meta.name);
        Ok(Self {
            local,
            name,
            config,
            remote,
        })
    }

    pub(crate) fn remote_rclone_target(&self) -> utils::rclone::RcloneTarget {
        self.config.as_rclone_target()
    }

    pub(crate) fn get_managed_repo(&self) -> Result<ManagedRepository, InternalError> {
        match self.remote.as_managed() {
            Some(tracked_remote) => Ok(tracked_remote),
            None => Err(AppError::NotManagedRemote {
                connection_name: self.name.clone(),
            }
            .into()),
        }
    }
}
