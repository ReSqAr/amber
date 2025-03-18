use anyhow::anyhow;
use std::path::Path;
use tokio::fs;

/// Helper to assert that a file exists. If `expected_content` is provided, its content is compared.
pub(crate) async fn assert_file_exists(
    dir: &Path,
    filename: &str,
    expected_content: &Option<String>,
) -> anyhow::Result<(), anyhow::Error> {
    let file_path = dir.join(filename);
    if !file_path.exists() {
        return Err(anyhow!("File {} does not exist", filename));
    }
    if let Some(expected) = expected_content {
        let content = fs::read_to_string(&file_path).await?;
        if content != *expected {
            return Err(anyhow!(
                "File {} content mismatch: expected '{}', got '{}'",
                filename,
                expected,
                content
            ));
        }
    }
    Ok(())
}

/// Helper to assert that a file does not exist.
pub(crate) async fn assert_file_does_not_exist(
    dir: &Path,
    filename: &str,
) -> anyhow::Result<(), anyhow::Error> {
    let file_path = dir.join(filename);
    if file_path.exists() {
        return Err(anyhow!("File {} exists, but it should not", filename));
    }
    Ok(())
}

/// Helper to assert that two files in the same directory are hardlinked.
/// On Unix-like systems this compares the inode numbers.
pub(crate) async fn assert_files_hardlinked(
    dir: &Path,
    file1: &str,
    file2: &str,
) -> anyhow::Result<(), anyhow::Error> {
    let path1 = dir.join(file1);
    let path2 = dir.join(file2);

    let meta1 = fs::metadata(&path1).await?;
    let meta2 = fs::metadata(&path2).await?;

    {
        use anyhow::anyhow;
        use std::os::unix::fs::MetadataExt;
        if meta1.ino() != meta2.ino() {
            return Err(anyhow!("Files {} and {} are not hardlinked", file1, file2));
        }
    }

    Ok(())
}

/// Helper to assert that two files in the same directory are NOT hardlinked.
/// On Unix-like systems this compares the inode numbers.
pub(crate) async fn assert_files_not_hardlinked(
    dir: &Path,
    file1: &str,
    file2: &str,
) -> anyhow::Result<(), anyhow::Error> {
    let path1 = dir.join(file1);
    let path2 = dir.join(file2);

    let meta1 = fs::metadata(&path1).await?;
    let meta2 = fs::metadata(&path2).await?;

    {
        use anyhow::anyhow;
        use std::os::unix::fs::MetadataExt;
        if meta1.ino() == meta2.ino() {
            return Err(anyhow!("Files {} and {} are hardlinked", file1, file2));
        }
    }

    Ok(())
}

/// Recursively compare non–hidden files in two directories.
pub(crate) async fn assert_directories_equal(
    dir1: &Path,
    dir2: &Path,
) -> anyhow::Result<(), anyhow::Error> {
    let mut entries1 = fs::read_dir(dir1).await?;
    let mut files1 = Vec::new();
    while let Some(entry) = entries1.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') {
            continue;
        }
        files1.push(name);
    }
    let mut entries2 = fs::read_dir(dir2).await?;
    let mut files2 = Vec::new();
    while let Some(entry) = entries2.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') {
            continue;
        }
        files2.push(name);
    }
    files1.sort();
    files2.sort();
    if files1 != files2 {
        return Err(anyhow!(
            "directory file lists differ: left={:?} vs right={:?}",
            files1,
            files2
        ));
    }
    for file in files1 {
        let path1 = dir1.join(&file);
        let path2 = dir2.join(&file);
        let meta1 = fs::metadata(&path1).await?;
        let meta2 = fs::metadata(&path2).await?;

        if meta1.is_dir() && meta2.is_dir() {
            // Wrap the recursive call to avoid infinite future size.
            Box::pin(assert_directories_equal(&path1, &path2)).await?;
        } else if meta1.is_file() && meta2.is_file() {
            let content1 = fs::read(&path1).await?;
            let content2 = fs::read(&path2).await?;
            if content1 != content2 {
                return Err(anyhow!("file content {} differs between directories", file));
            }
        } else {
            return Err(anyhow!(
                "type mismatch for {}: one is a file, the other is a directory",
                file
            ));
        }
    }
    Ok(())
}
