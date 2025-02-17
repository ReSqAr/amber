use log::debug;
use std::fmt::Debug;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use tokio::fs;

pub(crate) async fn are_hardlinked(
    path1: impl AsRef<Path> + Debug,
    path2: impl AsRef<Path> + Debug,
) -> std::io::Result<bool> {
    let metadata1 = fs::metadata(&path1).await?;
    let metadata2 = fs::metadata(&path2).await?;

    let result = metadata1.dev() == metadata2.dev() && metadata1.ino() == metadata2.ino();
    debug!(
        "are_hardlinked: path1: {:?}, path2: {:?} result {:}",
        path1, path2, result,
    );
    Ok(result)
}

#[cfg(test)]
mod tests {
    use crate::utils::fs::are_hardlinked;
    use tempfile::tempdir;
    use tokio::fs;

    #[tokio::test]
    async fn test_are_hardlinked() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("file.txt");
        fs::write(&file_path, "content").await?;

        let hardlink_path = dir.path().join("hardlink.txt");
        fs::hard_link(&file_path, &hardlink_path).await?;

        let linked = are_hardlinked(&file_path, &hardlink_path).await.unwrap();
        assert!(linked);

        Ok(())
    }
}
