use crate::repository::traits::{Metadata, RepositoryMetadata};
use crate::utils::errors::InternalError;

pub struct RCloneStore;

impl RCloneStore {
    pub async fn new() -> Result<Self, InternalError> {
        Ok(Self {}) // TODO
    }
}

impl Metadata for RCloneStore {
    async fn current(&self) -> Result<RepositoryMetadata, InternalError> {
        Ok(RepositoryMetadata {
            // TODO
            id: "".into(),
            name: "".into(),
        })
    }
}
