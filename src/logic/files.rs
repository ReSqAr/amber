use crate::repository::traits::Local;
use crate::utils::errors::InternalError;
use crate::utils::path::RepoPath;
use filetime::{FileTime, set_file_times};
use log::debug;
use std::path::Path;
use tokio::{fs, task};
use uuid::Uuid;

pub async fn create_hard_link(
    source: &RepoPath,
    destination: &RepoPath,
) -> Result<(), InternalError> {
    if let Some(parent) = destination.abs().parent() {
        fs::create_dir_all(parent).await?;
    }

    fs::hard_link(source, destination).await?;
    debug!(
        "hard linked {} to {}",
        source.display(),
        destination.display()
    );

    // make destination read only
    let mut permissions = fs::metadata(destination).await?.permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        fs::set_permissions(destination, permissions).await?;
    }

    Ok(())
}

pub async fn assimilate(source: &RepoPath, destination: &RepoPath) -> Result<(), InternalError> {
    if let Some(parent) = destination.abs().parent() {
        fs::create_dir_all(parent).await?;
    }

    // make destination read only
    let mut permissions = fs::metadata(&source).await?.permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        fs::set_permissions(&source, permissions).await?;
    }

    fs::rename(source, destination).await?;

    Ok(())
}

pub async fn forced_atomic_hard_link(
    local: &impl Local,
    source: &RepoPath,
    destination: &RepoPath,
    blob_id: &str,
) -> Result<(), InternalError> {
    // compute staging_path using blob_id and a unique UUID
    // we use the staging file to set the permissions/file times
    // before atomically overwriting the user's file
    let staging_path = local
        .staging_path()
        .join(format!("{}.{}", blob_id, Uuid::now_v7()));

    fs::hard_link(source, &staging_path).await?;
    debug!(
        "hard linked {} to {}",
        source.display(),
        staging_path.display()
    );

    let metadata = fs::metadata(source).await?;
    let mut permissions = metadata.permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&staging_path, permissions).await?;
    debug!("set permissions for {}", staging_path.display());

    let accessed_time = FileTime::from_last_access_time(&metadata);
    let modified_time = FileTime::from_last_modification_time(&metadata);
    let staging_path_clone = staging_path.clone();
    task::spawn_blocking(move || set_file_times(&staging_path_clone, accessed_time, modified_time))
        .await??;
    debug!("set file times for {}", staging_path.display());

    // atomically over-write the file - we trust upstream that the
    // file we overwrite has the same content as the existing file
    fs::rename(&staging_path, destination).await?;
    debug!(
        "atomically moved {} to {}",
        staging_path.display(),
        destination.display()
    );

    Ok(())
}

pub(crate) async fn cleanup_staging(staging_path: impl AsRef<Path>) -> Result<(), InternalError> {
    match fs::remove_dir_all(staging_path).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    };

    Ok(())
}
