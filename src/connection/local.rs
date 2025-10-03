use crate::repository::local::{LocalRepository, LocalRepositoryConfig};
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::InternalError;
use crate::utils::fs::Capability;
use crate::utils::rclone::{ConfigSection, RCloneTarget};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct LocalConfig {
    root: String,
}

impl LocalConfig {
    #[allow(clippy::result_large_err)]
    pub(crate) fn from_parameter(parameter: String) -> Result<Self, InternalError> {
        Ok(Self { root: parameter })
    }
    pub(crate) fn as_rclone_target(&self, remote_path: String) -> LocalTarget {
        LocalTarget { path: remote_path }
    }

    pub(crate) async fn connect(
        &self,
        app_folder: PathBuf,
        preferred_capability: Option<Capability>,
    ) -> Result<WrappedRepository, InternalError> {
        let LocalConfig { root } = self;
        let repository = LocalRepository::new(LocalRepositoryConfig {
            maybe_root: Some(root.clone().into()),
            app_folder,
            preferred_capability,
        })
        .await?;
        Ok(WrappedRepository::Local(repository))
    }
}

#[derive(Debug, Clone)]
pub struct LocalTarget {
    pub path: String,
}

impl RCloneTarget for LocalTarget {
    fn to_rclone_arg(&self) -> String {
        self.path.clone()
    }

    fn to_config_section(&self) -> ConfigSection {
        ConfigSection::None
    }
}
