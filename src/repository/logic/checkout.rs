use crate::db::models::FilePathWithBlobId;
use crate::repository::traits::{Local, Reconciler};
use anyhow::Context;
use futures::StreamExt;
use log::debug;
use tokio::fs;

pub async fn checkout(
    local: &(impl Local + Reconciler + Send),
) -> anyhow::Result<(), Box<dyn std::error::Error>> {
    let mut desired_state = local.target_filesystem_state();
    while let Some(next) = desired_state.next().await {
        let FilePathWithBlobId {
            path: relative_path,
            blob_id,
        } = next?;
        let object_path = local.blob_path(blob_id);

        let target_path = local.root().join(relative_path);
        debug!("trying hardlinking {:?} -> {:?}", object_path, target_path);

        if let Some(parent) = target_path.abs().parent() {
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
