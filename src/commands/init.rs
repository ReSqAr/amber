use crate::repository::local::LocalRepository;
use crate::utils::errors::InternalError;
use std::path::PathBuf;

pub async fn init_repository(maybe_root: Option<PathBuf>) -> Result<(), InternalError> {
    LocalRepository::create(maybe_root).await?;

    Ok(())
}
