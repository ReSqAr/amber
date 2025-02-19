use crate::repository::traits::{Adder, Local};
use crate::utils::errors::InternalError;
use crate::utils::path::RepoPath;
use std::os::unix::fs::PermissionsExt;
use tokio::fs;
use uuid::Uuid;

pub(crate) async fn unblobify(
    local: &(impl Adder + Local + Sized),
    file: &RepoPath,
) -> Result<(), InternalError> {
    // when unblobifying (soft delete) we want to make the file is writable again.
    // that also means we need to break any hard links. we do this by creating a copy.
    fs::create_dir_all(&local.staging_path()).await?;
    let staging_path = local
        .staging_path()
        .join(Uuid::now_v7().to_string())
        .abs()
        .clone();
    fs::copy(file, &staging_path).await?;

    let mut permissions = fs::metadata(&staging_path).await?.permissions();
    let current_mode = permissions.mode();
    let user_write_bit = 0o200;
    if current_mode & user_write_bit == 0 {
        permissions.set_mode(current_mode | user_write_bit);
        fs::set_permissions(&staging_path, permissions).await?;
    }

    fs::rename(&staging_path, file).await?;

    Ok(())
}
