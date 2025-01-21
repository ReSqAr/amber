use crate::repository::local::LocalRepository;

pub async fn init_repository() -> Result<(), Box<dyn std::error::Error>> {
    LocalRepository::create(None).await?;

    Ok(())
}
