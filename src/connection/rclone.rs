use crate::repository::local::LocalRepository;
use crate::repository::rclone::RCloneStore;
use crate::repository::traits::RcloneTargetPath;
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::{AppError, InternalError};
use crate::utils::rclone::{ConfigSection, RCloneTarget};

#[derive(Clone, Debug)]
pub struct RCloneRemoteConfig {
    pub remote_name: String,
    pub remote_path: String,
}

impl RCloneRemoteConfig {
    pub(crate) fn from_parameter(parameter: String) -> Result<Self, InternalError> {
        let (remote_name, remote_path) = parameter.rsplit_once(":").ok_or(AppError::Parse {
            message: "cannot extract remote path".into(),
            raw: parameter.clone(),
        })?;

        Ok(Self {
            remote_name: remote_name.into(),
            remote_path: remote_path.into(),
        })
    }

    pub(crate) fn as_rclone_target(&self, remote_path: String) -> RCloneRemoteTarget {
        RCloneRemoteTarget {
            remote_name: self.remote_name.clone(),
            remote_path,
        }
    }

    pub(crate) async fn connect(
        &self,
        local: &LocalRepository,
        name: &str,
    ) -> Result<WrappedRepository, InternalError> {
        let repository = RCloneStore::new(local, name, &self.remote_path).await?;
        Ok(WrappedRepository::RClone(repository))
    }
}

impl RcloneTargetPath for RCloneRemoteConfig {
    async fn rclone_path(&self, _transfer_id: u32) -> Result<String, InternalError> {
        Ok(self.remote_path.clone())
    }
}

#[derive(Debug, Clone)]
pub struct RCloneRemoteTarget {
    pub remote_name: String,
    pub remote_path: String,
}

impl RCloneTarget for RCloneRemoteTarget {
    fn to_rclone_arg(&self) -> String {
        format!("{}:{}", self.remote_name, self.remote_path)
    }

    fn to_config_section(&self) -> ConfigSection {
        ConfigSection::GlobalConfig
    }
}
