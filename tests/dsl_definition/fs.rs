use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tokio::io::AsyncWriteExt;

/// Helper to create a random file with random content.
pub(crate) async fn create_random_file(
    dir: &Path,
    filename: &str,
    size: usize,
) -> Result<(), anyhow::Error> {
    let file_path = dir.join(filename);
    let mut file = tokio::fs::File::create(&file_path).await?;
    let content: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
    file.write_all(&content).await?;
    Ok(())
}

/// Helper to write a file with specific content.
pub(crate) async fn write_file(
    dir: &Path,
    filename: &str,
    content: &str,
) -> Result<(), anyhow::Error> {
    let file_path = dir.join(filename);
    if let Some(parent) = file_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap_or_else(|_| {
            panic!(
                "unable to create parent directory for {}",
                file_path.display()
            )
        });
    }

    if file_path.exists() {
        let metadata = tokio::fs::metadata(&file_path).await?;
        let mut perms = metadata.permissions();

        if perms.readonly() {
            perms.set_mode(perms.mode() | 0o200);
            tokio::fs::set_permissions(&file_path, perms).await?;
        }
    }

    // Now write to the file
    tokio::fs::write(&file_path, content).await?;
    Ok(())
}

/// Helper to remove a file.
pub(crate) async fn remove_file(dir: &Path, filename: &str) -> Result<(), anyhow::Error> {
    let file_path = dir.join(filename);
    tokio::fs::remove_file(&file_path).await?;
    Ok(())
}
