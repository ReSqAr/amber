use crate::db::models::BlobID;
use crate::flightdeck::base::{BaseObservable, BaseObservation};
use crate::flightdeck::observer::Observer;
use crate::utils::path::RepoPath;
use std::io;
use tokio::fs;
use tokio::io::AsyncReadExt;

pub(crate) struct HashWithSize {
    pub(crate) hash: BlobID,
    pub(crate) size: u64,
}

pub(crate) async fn compute_blake3_and_size(file_path: &RepoPath) -> io::Result<HashWithSize> {
    let mut obs = Observer::with_auto_termination(
        BaseObservable::with_id("blake3", file_path.rel().display().to_string()),
        log::Level::Trace,
        BaseObservation::TerminalState("done".into()),
    );

    let mut file = fs::File::open(file_path).await?;
    obs.observe_length(log::Level::Trace, file.metadata().await?.len());

    let mut hasher = blake3::Hasher::new();
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

        let chunk = buffer
            .get(..bytes_read)
            .ok_or_else(|| io::Error::other("read beyond buffer size"))?;

        hasher.update(chunk);
        size += bytes_read as u64;
        obs.observe_position(log::Level::Trace, size);
    }

    let hash = hasher.finalize();

    Ok(HashWithSize {
        hash: BlobID(hash.to_hex().to_string()),
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
    async fn test_compute_blake3_and_size() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let path = RepoPath::from_root(dir.path());
        let file_path = path.join("hello.txt");
        {
            let mut file = fs::File::create(&file_path).await?;
            file.write_all(b"Hello world!").await?;
            file.flush().await?;
        }

        let result = compute_blake3_and_size(&file_path).await.unwrap();
        assert_eq!(
            result.hash.0,
            "793c10bc0b28c378330d39edace7260af9da81d603b8ffede2706a21eda893f4"
        );
        assert_eq!(result.size, 12u64);

        Ok(())
    }
}
