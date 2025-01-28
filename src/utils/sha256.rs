use sha2::{Digest, Sha256};
use std::io;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

pub(crate) async fn compute_sha256_and_size(
    file_path: impl AsRef<Path>,
) -> io::Result<(String, i64)> {
    let mut file = File::open(file_path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024]; // 1MB buffer
    let mut size = 0i64;

    loop {
        let bytes_read = file.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
        size += bytes_read as i64;
    }

    let hash = hasher.finalize();
    Ok((format!("{:x}", hash), size))
}
