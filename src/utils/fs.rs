use log::debug;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use tokio::fs;

pub(crate) async fn are_hardlinked(path1: &Path, path2: &Path) -> std::io::Result<bool> {
    let metadata1 = fs::metadata(path1).await?;
    let metadata2 = fs::metadata(path2).await?;

    let result = metadata1.dev() == metadata2.dev() && metadata1.ino() == metadata2.ino();
    debug!(
        "are_hardlinked: path1: {:?}, path2: {:?} result {:}",
        path1, path2, result,
    );
    Ok(result)
}
