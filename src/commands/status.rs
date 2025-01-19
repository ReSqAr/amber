use crate::repository::local_repository::LocalRepository;
use log::{error, info};

use futures::StreamExt;

use crate::repository::logic::state;
use crate::repository::logic::state::StateConfig;


pub async fn status() -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    let (handle, mut stream) = state::state(local_repository, StateConfig::default()).await?;
    let output_handle = tokio::spawn(async move {
        while let Some(file_result) = stream.next().await {
            match file_result {
                Ok(file_obs) => {
                    info!("file observed: {:?}", file_obs);
                }
                Err(e) => {
                    error!("Error during traversal: {}", e);
                }
            }
        }
    });

    handle.await??;
    output_handle.await?;

    Ok(())
}