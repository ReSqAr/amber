use crate::commands::errors::InvariableError;
use crate::db::db::DB;
use crate::db::establish_connection;
use crate::db::models::{CurrentRepository, FilePathWithObjectId};
use crate::db::schema::run_migrations;
use anyhow::{Context, Result};
use async_tempfile::TempDir;
use log::{debug, info};
use std::path::Path;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

pub async fn export(target_path: String) -> Result<(), Box<dyn std::error::Error>> {
    let current_path = fs::canonicalize(".").await?;
    let invariable_path = current_path.join(".inv");
    if !fs::metadata(&invariable_path)
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        return Err(InvariableError::NotInitialised().into());
    };

    let db_path = invariable_path.join("db.sqlite");
    let pool = establish_connection(db_path.to_str().unwrap())
        .await
        .context("failed to establish connection")?;
    run_migrations(&pool)
        .await
        .context("failed to run migrations")?;

    let db = DB::new(pool.clone());
    debug!("db connected");

    let CurrentRepository { repo_id } = db.get_or_create_current_repository().await?;
    debug!("local repo_id={}", repo_id);

    let staging_path = invariable_path.join("staging");
    fs::create_dir_all(&staging_path)
        .await
        .context("unable to create staging directory")?;
    let temp_dir = TempDir::new_in::<&Path>(&staging_path)
        .await
        .context("unable to create temp directory")?;
    let temp_dir_path = temp_dir.dir_path();

    let root = current_path;

    for FilePathWithObjectId {
        path: relative_path,
        object_id,
    } in db
        .desired_filesystem_state(&repo_id)
        .await
        .context("unable to determine desired fs")?
    {
        let invariable_path = root.join(".inv");
        let blob_path = invariable_path.join("blobs");
        let object_path = blob_path.join(object_id);

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
