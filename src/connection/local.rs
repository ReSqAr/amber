use crate::repository::local::LocalRepository;
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::InternalError;
use crate::utils::rclone;

#[derive(Clone, Debug)]
pub struct LocalConfig {
    root: String,
}

impl LocalConfig {
    pub(crate) fn from_parameter(parameter: String) -> Result<Self, InternalError> {
        Ok(Self { root: parameter })
    }

    pub(crate) fn as_rclone_target(&self, remote_path: String) -> rclone::RcloneTarget {
        rclone::RcloneTarget::Local(rclone::LocalConfig { path: remote_path })
    }

    pub(crate) async fn connect(&self) -> Result<WrappedRepository, InternalError> {
        let LocalConfig { root } = self;
        let repository = LocalRepository::new(Some(root.clone().into())).await?;
        Ok(WrappedRepository::Local(repository))
    }
}
