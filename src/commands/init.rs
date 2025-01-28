use crate::repository::local::LocalRepository;
use std::path::PathBuf;

pub async fn init_repository(
    maybe_root: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    LocalRepository::create(maybe_root).await?;

    Ok(())
}
