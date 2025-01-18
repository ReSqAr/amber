use crate::commands::errors::InvariableError;
use crate::db::db::DB;
use crate::db::establish_connection;
use crate::db::models::{CurrentRepository, FilePathWithObjectId};
use crate::db::schema::run_migrations;
use anyhow::{Context, Result};
use async_tempfile::TempDir;
use futures::StreamExt;
use log::{debug, info};
use std::path::Path;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use crate::repository::local_repository::LocalRepository;

pub async fn export(target_path: String) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    debug!("local repo_id={}", local_repository.repo_id);

    let staging_path = local_repository.invariable_path.join("staging");
    fs::create_dir_all(&staging_path)
        .await
        .context("unable to create staging directory")?;
    let temp_dir = TempDir::new_in::<&Path>(&staging_path)
        .await
        .context("unable to create temp directory")?;
    let temp_dir_path = temp_dir.dir_path();

    let root = local_repository.root;

    let mut desired_state = local_repository.db.desired_filesystem_state(local_repository.repo_id.clone());
    while let Some(next) = desired_state.next().await {
        let FilePathWithObjectId {
            path: relative_path,
            object_id,
        } = next?;

        let object_path = local_repository.blob_path.join(object_id);

        let target_path = temp_dir_path.join(relative_path);
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent)
                .await
                .context("unable to create parent directory")?;
        }

        fs::hard_link(&object_path, &target_path)
            .await
            .context("unable to hardlink files")?;
    }
    debug!("prepared staging_path={:?}", staging_path);

    let mut cmd = Command::new("rclone")
        .arg("copy")
        .arg(temp_dir_path.as_os_str())
        .arg(target_path)
        .arg("--use-json-log")
        .arg("--progress")
        .arg("--log-level")
        .arg("INFO")
        .stdout(std::process::Stdio::piped())
        .spawn()
        .context("rclone startup failed")?;

    let stdout = cmd
        .stdout
        .take()
        .context("child did not have a handle to stdout")?;

    let mut reader = BufReader::new(stdout).lines();

    while let Some(line) = reader
        .next_line()
        .await
        .context("rclone communication failed")?
    {
        debug!("Line: {}", line);
    }

    let status = cmd.wait().await.context("rclone failed")?;
    info!("Child process exited with status: {}", status);

    Ok(())
}
