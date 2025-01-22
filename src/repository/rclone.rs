use crate::repository::traits::Metadata;
use crate::utils::app_error::AppError;

pub type RCloneClient = ();

impl Metadata for RCloneClient {
    async fn repo_id(&self) -> Result<String, AppError> {
        Ok("".to_string())
    }
}
