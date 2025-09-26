use anyhow::{Context, anyhow};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
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

async fn collect_files(root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let mut rd = fs::read_dir(&dir)
            .await
            .with_context(|| format!("reading directory {}", dir.display()))?;

        while let Some(entry) = rd.next_entry().await? {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with('.') {
                continue;
            }

            let path = entry.path();
            let meta = fs::metadata(&path).await?;
            if meta.is_dir() {
                stack.push(path);
            } else if meta.is_file() {
                let rel = path
                    .strip_prefix(root)
                    .with_context(|| format!("strip_prefix {}", path.display()))?
                    .to_owned();
                files.push(rel);
            } else {
                return Err(anyhow!("unknown file type {}", path.display()));
            }
        }
    }

    Ok(files)
}

pub(crate) async fn assert_directories_equal(dir1: &Path, dir2: &Path) -> anyhow::Result<()> {
    let mut files1 = collect_files(dir1).await?;
    let mut files2 = collect_files(dir2).await?;

    files1.sort();
    files2.sort();

    if files1 != files2 {
        let set1: HashSet<_> = files1.iter().collect();
        let set2: HashSet<_> = files2.iter().collect();

        let only_left: Vec<_> = set1
            .difference(&set2)
            .map(|p| p.display().to_string())
            .collect();
        let only_right: Vec<_> = set2
            .difference(&set1)
            .map(|p| p.display().to_string())
            .collect();
        let both: Vec<_> = set2
            .intersection(&set2)
            .map(|p| p.display().to_string())
            .collect();

        return Err(anyhow!(
            "file sets differ\n  only in left:  {:?}\n  only in right: {:?}\n  in both: {:?}",
            only_left,
            only_right,
            both,
        ));
    }

    for rel in &files1 {
        let p1 = dir1.join(rel);
        let p2 = dir2.join(rel);

        let b1 = fs::read(&p1)
            .await
            .with_context(|| format!("reading {}", p1.display()))?;
        let b2 = fs::read(&p2)
            .await
            .with_context(|| format!("reading {}", p2.display()))?;
        if b1 != b2 {
            return Err(anyhow!("file content differs: {}", rel.display()));
        }
    }

    Ok(())
}
