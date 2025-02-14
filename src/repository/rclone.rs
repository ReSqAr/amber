use crate::repository::traits::{Metadata, RepositoryMetadata};
use crate::utils::errors::InternalError;

pub type RCloneClient = ();

impl Metadata for RCloneClient {
    async fn current(&self) -> Result<RepositoryMetadata, InternalError> {
        Ok(RepositoryMetadata {
            id: "".into(),
            name: "".into(),
        })
    }
}
