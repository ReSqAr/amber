use crate::utils::errors::InternalError;
use log::debug;
use std::fmt::Debug;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use tokio::{fs, task};

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub enum Capability {
    RefLinks,
    HardLinks,
}

pub async fn capability_check(
    path: &Path,
    preferred: Option<Capability>,
) -> Result<Option<Capability>, InternalError> {
    let order = match preferred {
        Some(Capability::RefLinks) | None => vec![Capability::RefLinks, Capability::HardLinks],
        Some(Capability::HardLinks) => vec![Capability::HardLinks, Capability::RefLinks],
    };

    for capability in order {
        match capability {
            Capability::RefLinks => {
                if capability_check_ref_link(path).await? {
                    return Ok(Some(Capability::RefLinks));
                }
            }
            Capability::HardLinks => {
                if capability_check_hard_link(path).await? {
                    return Ok(Some(Capability::HardLinks));
                }
            }
        }
    }
    Ok(None)
}

async fn capability_check_ref_link(path: &Path) -> Result<bool, InternalError> {
    let source = path.join(".capability_check_reflink_source");
    let destination = path.join(".capability_check_reflink_destination");

    {
        fs::File::create(&source).await?;
    }

    let source_cloned = source.clone();
    let destination_cloned = destination.clone();
    let result =
        task::spawn_blocking(move || reflink_copy::reflink(&source_cloned, &destination_cloned))
            .await?;

    let _ = fs::remove_file(&source).await;
    let _ = fs::remove_file(&destination).await;
    Ok(result.is_ok())
}

async fn capability_check_hard_link(path: &Path) -> Result<bool, InternalError> {
    let source = path.join(".capability_check_hardlink_source");
    let destination = path.join(".capability_check_hardlink_destination");

    {
        fs::File::create(&source).await?;
    }

    let result = fs::hard_link(&source, &destination).await;

    let _ = fs::remove_file(&source).await;
    let _ = fs::remove_file(&destination).await;
    Ok(result.is_ok())
}

pub async fn link(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    capability: &Capability,
) -> Result<(), InternalError> {
    match capability {
        Capability::RefLinks => {
            let source = source.as_ref().to_path_buf();
            let destination = destination.as_ref().to_path_buf();
            Ok(
                task::spawn_blocking(move || reflink_copy::reflink(&source, &destination))
                    .await??,
            )
        }
        Capability::HardLinks => Ok(fs::hard_link(&source, &destination).await?),
    }
}

pub(crate) async fn are_hard_linked(
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
    use crate::utils::fs::{Capability, are_hard_linked, link};
    use tempfile::tempdir;
    use tokio::fs;

    #[tokio::test]
    async fn test_are_hardlinked() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("file.txt");
        fs::write(&file_path, "content").await?;

        let hardlink_path = dir.path().join("hardlink.txt");
        link(&file_path, &hardlink_path, &Capability::HardLinks).await?;

        let linked = are_hard_linked(&file_path, &hardlink_path).await.unwrap();
        assert!(linked);

        Ok(())
    }
}
