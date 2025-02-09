use crate::db::models::ConnectionType;
use crate::repository::local::LocalRepository;
use crate::repository::logic::connect;
use crate::repository::traits::Metadata;
use crate::repository::wrapper::{ManagedRepository, WrappedRepository};
use crate::utils::errors::{AppError, InternalError};
use crate::utils::rclone;
use log::debug;

pub struct EstablishedConnection {
    pub name: String,
    pub config: connect::ConnectionConfig,
    pub local: LocalRepository,
    pub remote: WrappedRepository,
}

impl EstablishedConnection {
    pub async fn connect(
        local: LocalRepository,
        name: String,
        connection_type: ConnectionType,
        parameter: String,
    ) -> Result<Self, InternalError> {
        let config = connect::parse_config(connection_type, parameter)?;
        let remote = connect::connect(&config).await?;
        let repo_id = remote.repo_id().await?;
        debug!("connected to repository via {name}: {repo_id}");
        Ok(Self {
            local,
            name,
            config,
            remote,
        })
    }

    pub(crate) fn remote_rclone_target(&self) -> rclone::RcloneTarget {
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
