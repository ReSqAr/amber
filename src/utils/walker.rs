use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use ignore::overrides::OverrideBuilder;
use ignore::{DirEntry, WalkBuilder, WalkState};
use std::path::PathBuf;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use thiserror::Error;

pub struct WalkerConfig {
    pub threads: usize,
    pub max_buffer_size: usize,
    pub patterns: Vec<String>,
}

impl Default for WalkerConfig {
    fn default() -> Self {
        Self {
            threads: 0,
            max_buffer_size: 1000,
            patterns: vec!["!.inv/".into()],
        }
    }
}

#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error("I/O error")]
    IOError(#[from] std::io::Error),
    #[error("walker error")]
    IgnoreError(#[from] ignore::Error),
    #[error("observer error")]
    ObserverError(String),
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileObservation {
    pub rel_path: PathBuf,
    pub size: i64,
    pub last_modified: i64,
}

fn observe_dir_entry(root: &PathBuf, entry: DirEntry) -> Option<Result<FileObservation, Error>> {
    if !entry.file_type().map_or(false, |ft| ft.is_file()) {
        return None;
    }

    let rel_path = match entry.path().strip_prefix(root) {
        Ok(rel_path) => rel_path,
        Err(_) => {
            return Some(Err(Error::ObserverError(format!(
                "Cannot transform to relative path: {}",
                entry.path().display()
            ))));
        }
    }
    .to_path_buf();

    let metadata = match entry.metadata() {
        Ok(meta) => meta,
        Err(e) => {
            return Some(Err(Error::ObserverError(format!(
                "Failed to get metadata for {}: {}",
                rel_path.display(),
                e
            ))));
        }
    };

    let size = metadata.len();
    let last_modified = match metadata.modified() {
        Ok(time) => match time.duration_since(std::time::UNIX_EPOCH) {
            Ok(dur) => dur.as_secs() as i64,
            Err(e) => {
                return Some(Err(Error::ObserverError(format!(
                    "SystemTime before UNIX_EPOCH for {}: {}",
                    rel_path.display(),
                    e
                ))));
            }
        },
        Err(e) => {
            return Some(Err(Error::ObserverError(format!(
                "Failed to get modified time for {}: {}",
                rel_path.display(),
                e
            ))));
        }
    };
    Some(Ok(FileObservation {
        rel_path,
        size: size as i64,
        last_modified,
    }))
}

pub async fn walk<'a>(
    root_path: PathBuf,
    config: WalkerConfig,
) -> Result<(JoinHandle<()>, Receiver<Result<FileObservation, Error>>), Box<dyn std::error::Error>>
{
    let root = root_path.to_path_buf();

    let mut override_builder = OverrideBuilder::new(&root);
    for pattern in config.patterns {
        override_builder.add(pattern.as_str())?;
    }

    let mut walk_builder = WalkBuilder::new(&root);
    let walk_builder = walk_builder
        .standard_filters(true)
        .hidden(false)
        .follow_links(false)
        .same_file_system(true)
        .max_depth(None)
        .threads(config.threads)
        .overrides(override_builder.build()?);

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
                    Err(e) => Some(Err(Error::ObserverError(format!("Walk error: {}", e),
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
