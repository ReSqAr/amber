use crate::repository::local::LocalRepository;
use crate::repository::logic::checkout;
use crate::repository::logic::transfer::transfer;
use crate::repository::traits::{ConnectionManager, Local};
use crate::utils::errors::{AppError, InternalError};
use anyhow::Result;
use std::path::PathBuf;
use tokio::fs;

pub async fn pull(
    maybe_root: Option<PathBuf>,
    connection_name: String,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(maybe_root).await?;
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

    match fs::remove_dir_all(local.staging_path()).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    };

    Ok(())
}
