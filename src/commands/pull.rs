use crate::repository::local::LocalRepository;
use crate::repository::logic::checkout;
use crate::repository::logic::transfer::transfer;
use crate::repository::traits::ConnectionManager;
use crate::utils::errors::AppError;
use anyhow::Result;

pub async fn pull(connection_name: String) -> Result<(), Box<dyn std::error::Error>> {
    let local = LocalRepository::new(None).await?;
    let connection = local.connect(connection_name.clone()).await?;
    let managed_remote = match connection.remote.as_managed() {
        Some(tracked_remote) => tracked_remote,
        None => {
            return Err(AppError::UnsupportedRemote {
                connection_name,
                operation: "sync".into(),
            }
            .into());
        }
    };

    transfer(&local, &managed_remote, &local, connection).await?;

    checkout::checkout(&local).await?;

    Ok(())
}
