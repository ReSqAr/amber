use crate::repository::local::LocalRepository;
use crate::repository::traits::{Metadata, RepositoryMetadata};
use crate::utils::errors::InternalError;
use uuid::Uuid;

#[derive(Clone)]
pub struct RCloneStore {
    local: LocalRepository,
    name: String,
}

impl RCloneStore {
    pub async fn new(local: &LocalRepository, name: &str) -> Result<Self, InternalError> {
        Ok(Self {
            local: local.clone(),
            name: name.into(),
        })
    }
}

impl Metadata for RCloneStore {
    async fn current(&self) -> Result<RepositoryMetadata, InternalError> {
        Ok(RepositoryMetadata {
            id: Uuid::new_v5(&Uuid::NAMESPACE_OID, self.name.as_ref()).to_string(),
            name: self.name.clone(),
        })
    }
}
