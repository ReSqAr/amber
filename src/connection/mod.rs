use crate::connection::local::LocalTarget;
use crate::db::models::ConnectionType;
use crate::repository::local::LocalRepository;
use crate::repository::traits::Metadata;
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::InternalError;
use config::{Config, ConnectionTarget};
use log::debug;

mod config;
mod local;
mod rclone;
pub(crate) mod ssh;

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

    pub(crate) fn local_rclone_target(&self, path: String) -> ConnectionTarget {
        ConnectionTarget::Local(LocalTarget { path })
    }
    pub(crate) fn remote_rclone_target(&self, remote_path: String) -> ConnectionTarget {
        self.config.as_rclone_target(remote_path)
    }
}
