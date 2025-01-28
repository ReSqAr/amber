use crate::repository::local::LocalRepository;
use crate::repository::logic::transfer::transfer;
use crate::repository::traits::ConnectionManager;
use crate::utils::errors::AppError;
use anyhow::Result;

pub async fn push(connection_name: String) -> Result<(), Box<dyn std::error::Error>> {
    let local = LocalRepository::new(None).await?;
    let connection = local.connect(connection_name.clone()).await?;
    let tracked_remote = match connection.repository.as_tracked() {
        Some(tracked_remote) => tracked_remote,
        None => {
            return Err(AppError::UnsupportedRemote(format!(
                "{} does not support pull",
                connection_name
            ))
            .into());
        }
    };

    transfer(&local, &local, &tracked_remote, connection).await?;

    Ok(())
}
