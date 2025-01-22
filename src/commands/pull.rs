use crate::repository::local::LocalRepository;
use crate::repository::logic::checkout;
use crate::repository::traits::Metadata;
use anyhow::Result;
use log::debug;

pub async fn pull(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;
    let local_repo_id = local_repository.repo_id().await?;
    debug!("local repo_id={}", local_repo_id);
    // TODO

    checkout::checkout(&local_repository).await?;

    Ok(())
}
