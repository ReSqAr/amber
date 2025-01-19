use crate::repository::local_repository::LocalRepository;
use crate::repository::traits::Local;
use crate::utils::walker::{walk, WalkerConfig};
use log::{error, info};

pub async fn status() -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    let (handle, mut rx) = walk(&local_repository.root(), WalkerConfig::default()).await?;

    tokio::spawn(async move {
        while let Some(file_result) = rx.recv().await {
            match file_result {
                Ok(file_obs) => {
                    info!(
                        "Discovered file: {:?}, Size: {} bytes, Last Modified: {}",
                        file_obs.rel_path, file_obs.size, file_obs.last_modified
                    );
                }
                Err(e) => {
                    error!("Error during traversal: {}", e);
                }
            }
        }

        info!("Walker has completed traversing the directory.");
    });

    handle.await?;

    Ok(())
}
