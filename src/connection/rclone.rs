use crate::repository::rclone::RCloneStore;
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::{AppError, InternalError};
use crate::utils::rclone;
use base64::Engine;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct RCloneConfig {
    pub config: String,
    pub remote_path: PathBuf,
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

    pub(crate) fn as_rclone_target(&self) -> rclone::RcloneTarget {
        rclone::RcloneTarget::RClone(rclone::RCloneConfig {
            config: self.config.clone(),
            remote_path: self.remote_path.clone(),
        })
    }

    pub(crate) async fn connect(&self) -> Result<WrappedRepository, InternalError> {
        let repository = RCloneStore::new().await?;
        Ok(WrappedRepository::RClone(repository))
    }
}
