use crate::flightdeck::base::{BaseObservable, BaseObservation};
use crate::flightdeck::observer::Observer;
use sha2::{Digest, Sha256};
use std::io;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

pub(crate) struct HashWithSize {
    pub(crate) hash: String,
    pub(crate) size: u64,
}

pub(crate) async fn compute_sha256_and_size(
    file_path: impl AsRef<Path> + Clone,
) -> io::Result<HashWithSize> {
    let mut obs = Observer::with_auto_termination(
        BaseObservable::with_id("sha", file_path.clone().as_ref().display().to_string()),
        log::Level::Trace,
        BaseObservation::TerminalState("done".into()),
    );

    let mut file = File::open(file_path).await?;
    obs.observe_length(log::Level::Trace, file.metadata().await?.len());

    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024]; // 1MB buffer
    let mut size = 0u64;

    loop {
        let bytes_read = file.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
        size += bytes_read as u64;
        obs.observe_position(log::Level::Trace, size);
    }

    let hash = hasher.finalize();
    Ok(HashWithSize {
        hash: format!("{:x}", hash),
        size,
    })
}
