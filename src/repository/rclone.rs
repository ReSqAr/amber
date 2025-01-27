use crate::repository::traits::Metadata;
use crate::utils::errors::InternalError;

pub type RCloneClient = ();

impl Metadata for RCloneClient {
    async fn repo_id(&self) -> Result<String, InternalError> {
        Ok("".to_string())
    }
}
