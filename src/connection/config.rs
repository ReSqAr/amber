use crate::connection::local::{LocalConfig, LocalTarget};
use crate::connection::rclone::{RCloneRemoteConfig, RCloneRemoteTarget};
use crate::connection::ssh::{SshConfig, SshTarget};
use crate::db::models::ConnectionType;
use crate::repository::local::LocalRepository;
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::InternalError;
use crate::utils::rclone::{ConfigSection, RCloneTarget};

#[derive(Clone, Debug)]
pub enum Config {
    Local(LocalConfig),
    Ssh(SshConfig),
    RClone(RCloneRemoteConfig),
}

impl Config {
    #[allow(clippy::result_large_err)]
    pub(crate) fn parse(
        connection_type: ConnectionType,
        parameter: String,
    ) -> Result<Self, InternalError> {
        match connection_type {
            ConnectionType::Local => Ok(Config::Local(LocalConfig::from_parameter(parameter)?)),
            ConnectionType::RClone => Ok(Config::RClone(RCloneRemoteConfig::from_parameter(
                parameter,
            )?)),
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

    pub(crate) fn as_rclone_target(&self, remote_path: String) -> ConnectionTarget {
        match self {
            Config::Local(local_config) => {
                ConnectionTarget::Local(local_config.as_rclone_target(remote_path))
            }
            Config::RClone(rclone_config) => {
                ConnectionTarget::RClone(rclone_config.as_rclone_target(remote_path))
            }
            Config::Ssh(ssh_config) => {
                ConnectionTarget::Ssh(ssh_config.as_rclone_target(remote_path))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConnectionTarget {
    Local(LocalTarget),
    RClone(RCloneRemoteTarget),
    Ssh(SshTarget),
}

impl RCloneTarget for ConnectionTarget {
    fn to_rclone_arg(&self) -> String {
        match self {
            ConnectionTarget::Local(cfg) => cfg.to_rclone_arg(),
            ConnectionTarget::RClone(cfg) => cfg.to_rclone_arg(),
            ConnectionTarget::Ssh(cfg) => cfg.to_rclone_arg(),
        }
    }

    fn to_config_section(&self) -> ConfigSection {
        match self {
            ConnectionTarget::Local(cfg) => cfg.to_config_section(),
            ConnectionTarget::RClone(cfg) => cfg.to_config_section(),
            ConnectionTarget::Ssh(cfg) => cfg.to_config_section(),
        }
    }
}
