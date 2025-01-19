use serde::{Deserialize, Serialize};
use std::io::Error;

use ignore::{DirEntry, WalkBuilder, WalkState};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

pub struct WalkerConfig {
    pub max_concurrency: usize,
    pub max_buffer_size: usize,
}

impl Default for WalkerConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 10,
            max_buffer_size: 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileObservation {
    pub rel_path: PathBuf,
    pub size: u64,
    pub last_modified: i64,
}

fn observe_dir_entry(root: &PathBuf, entry: DirEntry) -> Option<Result<FileObservation, Error>> {
    if !entry.file_type().map_or(false, |ft| ft.is_file()) {
        return None;
    }

    let rel_path = match entry.path().strip_prefix(root) {
        Ok(rel_path) => rel_path,
        Err(_) => {
            let err = Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Cannot transform to relative path: {}",
                    entry.path().display()
                ),
            );
            return Some(Err(err));
        }
    }
    .to_path_buf();

    let metadata = match entry.metadata() {
        Ok(meta) => meta,
        Err(e) => {
            let err = Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to get metadata for {}: {}", rel_path.display(), e),
            );
            return Some(Err(err));
        }
    };

    let size = metadata.len();
    let last_modified = match metadata.modified() {
        Ok(time) => match time.duration_since(std::time::UNIX_EPOCH) {
            Ok(dur) => dur.as_secs() as i64,
            Err(e) => {
                let err = Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "SystemTime before UNIX_EPOCH for {}: {}",
                        rel_path.display(),
                        e
                    ),
                );
                return Some(Err(err));
            }
        },
        Err(e) => {
            let err = Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Failed to get modified time for {}: {}",
                    rel_path.display(),
                    e
                ),
            );
            return Some(Err(err));
        }
    };
    Some(Ok(FileObservation {
        rel_path,
        size,
        last_modified,
    }))
}

pub async fn walk<'a>(
    root_path: &'a Path,
    config: WalkerConfig,
) -> Result<(JoinHandle<()>, Receiver<Result<FileObservation, Error>>), Error> {
    let root = root_path.to_path_buf();

    let mut walk_builder = WalkBuilder::new(&root);
    let walk_builder = walk_builder
        .standard_filters(true)
        .hidden(false)
        .max_depth(None)
        .threads(config.max_concurrency);

    let walker = walk_builder.build_parallel();
    let (tx, rx): (
        Sender<Result<FileObservation, Error>>,
        Receiver<Result<FileObservation, Error>>,
    ) = mpsc::channel(config.max_buffer_size);
    let handle: JoinHandle<()> = tokio::task::spawn_blocking(move || {
        walker.run(|| {
            let root = root.clone();
            let tx = tx.clone();
            Box::new(move |result| {
                let obs = match result {
                    Ok(entry) => observe_dir_entry(&root, entry),
                    Err(e) => Some(Err(Error::new(
                        std::io::ErrorKind::Other,
                        format!("Walk error: {}", e),
                    ))),
                };
                if let Some(observation) = obs {
                    let _ = tx.blocking_send(observation);
                }
                WalkState::Continue
            })
        });
        drop(tx);
    });

    Ok((handle, rx))
}
