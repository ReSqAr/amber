use crate::flightdeck;
use crate::utils::errors::InternalError;
use chrono::{DateTime, Utc};
use futures::Stream;
use ignore::overrides::OverrideBuilder;
use ignore::{DirEntry, WalkBuilder, WalkState};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::path::PathBuf;
use thiserror::Error;
use tokio::task::JoinHandle;

pub struct WalkerConfig {
    pub patterns: Vec<String>,
}

impl Default for WalkerConfig {
    fn default() -> Self {
        Self {
            patterns: vec!["!.amb/".into(), "!.git/".into()],
        }
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
    #[error("walker error: {0}")]
    Ignore(#[from] ignore::Error),
    #[error("observer error")]
    Observer(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileObservation {
    pub rel_path: PathBuf,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
}

fn observe_dir_entry(root: &PathBuf, entry: DirEntry) -> Option<Result<FileObservation, Error>> {
    if !entry.file_type().is_some_and(|ft| ft.is_file()) {
        return None;
    }

    let rel_path = match entry.path().strip_prefix(root) {
        Ok(rel_path) => rel_path,
        Err(_) => {
            return Some(Err(Error::Observer(format!(
                "Cannot transform to relative path: {}",
                entry.path().display()
            ))));
        }
    }
    .to_path_buf();

    let metadata = match entry.metadata() {
        Ok(meta) => meta,
        Err(e) => {
            return Some(Err(Error::Observer(format!(
                "Failed to get metadata for {}: {}",
                rel_path.display(),
                e
            ))));
        }
    };

    let size = metadata.len();
    let last_modified = match metadata.modified() {
        Ok(time) => time.into(),
        Err(e) => {
            return Some(Err(Error::Observer(format!(
                "Failed to get modified time for {}: {}",
                rel_path.display(),
                e
            ))));
        }
    };
    Some(Ok(FileObservation {
        rel_path,
        size,
        last_modified,
    }))
}

pub async fn walk(
    root_path: PathBuf,
    config: WalkerConfig,
    buffer_size: usize,
) -> Result<
    (
        JoinHandle<()>,
        impl Stream<Item = Result<FileObservation, Error>>,
    ),
    InternalError,
> {
    let root = root_path.to_path_buf();

    let mut override_builder = OverrideBuilder::new(&root);
    for pattern in config.patterns {
        override_builder
            .add(pattern.as_str())
            .map_err(Into::<InternalError>::into)?;
    }

    let mut walk_builder = WalkBuilder::new(&root);
    let walk_builder = walk_builder
        .standard_filters(true)
        .hidden(true)
        .follow_links(false)
        .same_file_system(true)
        .max_depth(None)
        .overrides(
            override_builder
                .build()
                .map_err(Into::<InternalError>::into)?,
        );

    let walker = walk_builder.build_parallel();
    let (tx, rx) = flightdeck::tracked::mpsc_channel("walk", buffer_size);
    let handle: JoinHandle<()> = tokio::task::spawn_blocking(move || {
        walker.run(|| {
            let root = root.clone();
            let tx = tx.clone();
            Box::new(move |result| {
                let obs = match result {
                    Ok(entry) => observe_dir_entry(&root, entry),
                    Err(e) => Some(Err(Error::Observer(format!("Walk error: {e}")))),
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tempfile::tempdir;
    use tokio::fs;

    #[tokio::test]
    async fn test_walker() {
        let temp_dir = tempdir().expect("failed to create temporary directory");
        let dir_path = temp_dir.path().to_path_buf();

        let file1 = dir_path.join("file1.txt");
        let file2 = dir_path.join("file2.log");
        fs::write(&file1, b"content file 1")
            .await
            .expect("failed to write file1");
        fs::write(&file2, b"content file 2")
            .await
            .expect("failed to write file2");

        let amb_dir = dir_path.join(".amb");
        fs::create_dir_all(&amb_dir)
            .await
            .expect("failed to create .amb directory");
        let amb_file = amb_dir.join("hidden.txt");
        fs::write(&amb_file, b"hidden content")
            .await
            .expect("failed to write hidden file");

        let config = WalkerConfig::default();
        let (handle, mut rx) = walk(dir_path.clone(), config, 10)
            .await
            .expect("failed to start walker");

        let mut found_files = Vec::new();

        while let Some(result) = rx.next().await {
            match result {
                Ok(file_obs) => {
                    found_files.push(file_obs.rel_path.to_string_lossy().into_owned());
                }
                Err(e) => panic!("walker encountered an error: {:?}", e),
            }
        }

        handle.await.expect("walker task failed");

        assert!(
            found_files.iter().any(|s| s == "file1.txt"),
            "file1.txt not found in walker output: {:?}",
            found_files
        );
        assert!(
            found_files.iter().any(|s| s == "file2.log"),
            "file2.log not found in walker output: {:?}",
            found_files
        );
        assert!(
            !found_files.iter().any(|s| s.contains(".amb")),
            "Files from .amb directory should have been excluded, found: {:?}",
            found_files
        );
    }
}
