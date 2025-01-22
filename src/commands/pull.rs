use crate::db::models::FilePathWithBlobId;
use crate::repository::local::LocalRepository;
use crate::repository::traits::{Local, Metadata, Reconciler};
use anyhow::{Context, Result};
use futures::StreamExt;
use log::debug;
use tokio::fs;

pub async fn pull(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;
    let local_repo_id = local_repository.repo_id().await?;
    debug!("local repo_id={}", local_repo_id);
    // TODO

    reconcile_filesystem(&local_repository).await?;

    Ok(())
}

async fn reconcile_filesystem(
    local_repository: &LocalRepository,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut desired_state = local_repository.target_filesystem_state();
    while let Some(next) = desired_state.next().await {
        let FilePathWithBlobId {
            path: relative_path,
            blob_id,
        } = next?;
        let object_path = local_repository.blob_path(blob_id);

        let target_path = local_repository.root().join(relative_path);
        debug!("trying hardlinking {:?} -> {:?}", object_path, target_path);

        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        if !fs::metadata(&target_path)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            fs::hard_link(&object_path, &target_path)
                .await
                .context("unable to hardlink files")?;
            debug!("hardlinked {:?} -> {:?}", object_path, target_path);
        } else {
            debug!("skipped hardlinked {:?} -> {:?}", object_path, target_path);
        };
    }

    debug!("reconciling filesystem");

    Ok(())
}
