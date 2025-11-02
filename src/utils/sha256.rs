use crate::db::models::BlobID;
use crate::flightdeck::base::{BaseObservable, BaseObservation};
use crate::flightdeck::observer::Observer;
use crate::utils::path::RepoPath;
use sha2::{Digest, Sha256};
use std::io;
use tokio::fs;
use tokio::io::AsyncReadExt;

pub(crate) struct HashWithSize {
    pub(crate) hash: BlobID,
    pub(crate) size: u64,
}

pub(crate) async fn compute_sha256_and_size(file_path: &RepoPath) -> io::Result<HashWithSize> {
    let mut obs = Observer::with_auto_termination(
        BaseObservable::with_id("sha", file_path.rel().display().to_string()),
        log::Level::Trace,
        BaseObservation::TerminalState("done".into()),
    );

    let mut file = fs::File::open(file_path).await?;
    obs.observe_length(log::Level::Trace, file.metadata().await?.len());

    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024]; // 1MB buffer
    let mut size = 0u64;

    loop {
        let bytes_read = file
            .read(&mut buffer)
            .await
            .map_err(obs.observe_error(|_| BaseObservation::TerminalState("error".into())))?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
        size += bytes_read as u64;
        obs.observe_position(log::Level::Trace, size);
    }

    let hash = hasher.finalize();
    Ok(HashWithSize {
        hash: BlobID(format!("{:x}", hash)),
        size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::fs;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_compute_sha256_and_size() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let path = RepoPath::from_root(dir.path());
        let file_path = path.join("hello.txt");
        let mut file = fs::File::create(&file_path).await?;
        file.write_all(b"Hello world!").await?;

        let result = compute_sha256_and_size(&file_path).await.unwrap();
        assert_eq!(
            result.hash.0,
            "c0535e4be2b79ffd93291305436bf889314e4a3faec05ecffcbb7df31ad9e51a"
        );
        assert_eq!(result.size, 12u64);

        Ok(())
    }
}
