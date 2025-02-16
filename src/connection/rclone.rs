use crate::repository::local::LocalRepository;
use crate::repository::rclone::RCloneStore;
use crate::repository::traits::RcloneTargetPath;
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::{AppError, InternalError};
use crate::utils::rclone;
use base64::Engine;

#[derive(Clone, Debug)]
pub struct RCloneConfig {
    pub config: String,
    pub remote_path: String,
}

impl RCloneConfig {
    pub(crate) fn from_parameter(parameter: String) -> Result<Self, InternalError> {
        let (config_base64, remote_path) = parameter.split_once(":").ok_or(AppError::Parse {
            message: "cannot extract remote path".into(),
            raw: parameter.clone(),
        })?;

        let engine = base64::engine::GeneralPurpose::new(
            &base64::alphabet::STANDARD,
            base64::engine::general_purpose::NO_PAD,
        );
        let decoded = engine.decode(config_base64).unwrap();
        let config = String::from_utf8(decoded).unwrap();

        Ok(Self {
            config,
            remote_path: remote_path.into(),
        })
    }

    pub(crate) fn as_rclone_target(&self, remote_path: String) -> rclone::RcloneTarget {
        rclone::RcloneTarget::RClone(rclone::RCloneConfig {
            config: self.config.clone(),
            remote_path,
        })
    }

    pub(crate) async fn connect(
        &self,
        local: &LocalRepository,
        name: &str,
    ) -> Result<WrappedRepository, InternalError> {
        let repository = RCloneStore::new(local, name).await?;
        Ok(WrappedRepository::RClone(repository))
    }
}

impl RcloneTargetPath for RCloneConfig {
    async fn rclone_path(&self, _transfer_id: u32) -> Result<String, InternalError> {
        Ok(self.remote_path.clone())
    }
}
