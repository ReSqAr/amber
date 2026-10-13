use crate::flightdeck;
use crate::flightdeck::tracer::Tracer;
use crate::utils::errors::InternalError;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures_core::stream::BoxStream;
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

/// The sender the walk feeds its observations into.
type WalkSender = flightdeck::tracked::sender::TrackedSender<
    Result<FileObservation, Error>,
    flightdeck::tracked::sender::Adapter,
>;

/// Hands one observation to the consumer and says whether to keep walking.
///
/// The receiver is dropped as soon as the consumer stops reading - an early
/// return, an error raised further down, a `take(n)`. Walking the rest of the
/// tree after that only produces items that go straight in the bin, so the
/// walk stops instead.
fn forward(tx: &WalkSender, obs: Option<Result<FileObservation, Error>>) -> WalkState {
    let Some(observation) = obs else {
        return WalkState::Continue;
    };

    if tx.blocking_send(observation).is_err() {
        log::debug!("walk: output stream closed - stopping");
        return WalkState::Quit;
    }

    WalkState::Continue
}

pub async fn walk<'a>(
    root_path: PathBuf,
    config: WalkerConfig,
    buffer_size: usize,
) -> Result<
    (
        JoinHandle<()>,
        BoxStream<'a, Result<FileObservation, Error>>,
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
    let overrides = override_builder
        .build()
        .map_err(Into::<InternalError>::into)?;

    let (tx, rx) = flightdeck::tracked::mpsc_channel("walk", buffer_size);
    let handle: JoinHandle<()> = tokio::task::spawn_blocking(move || {
        let root = root;

        let tracer = Tracer::new_on("walk::builder");
        let mut walk_builder = WalkBuilder::new(&root);
        let walk_builder = walk_builder
            .standard_filters(true)
            .hidden(true)
            .follow_links(false)
            .same_file_system(true)
            .max_depth(None)
            .overrides(overrides);
        let walker = walk_builder.build_parallel();
        tracer.measure();

        let tracer = Tracer::new_on("walk::run::total");
        walker.run(|| {
            let root = root.clone();
            let tx = tx.clone();
            Box::new(move |result| {
                let obs = match result {
                    Ok(entry) => observe_dir_entry(&root, entry),
                    Err(e) => Some(Err(Error::Observer(format!("Walk error: {e}")))),
                };
                forward(&tx, obs)
            })
        });
        drop(tx);
        tracer.measure();
    });

    Ok((handle, rx.boxed()))
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

    fn an_observation() -> Option<Result<FileObservation, Error>> {
        Some(Ok(FileObservation {
            rel_path: "file.txt".into(),
            size: 1,
            last_modified: Utc::now(),
        }))
    }

    /// `forward` blocks, so - like the walk itself - it runs off the runtime
    /// threads.
    async fn forward_off_the_runtime(
        tx: WalkSender,
        obs: Option<Result<FileObservation, Error>>,
    ) -> WalkState {
        tokio::task::spawn_blocking(move || forward(&tx, obs))
            .await
            .expect("forward")
    }

    /// A consumer that stops reading - an early return, an error further down,
    /// a `take(n)` - drops the receiver. The walk used to carry on stat-ing the
    /// whole tree and throw every result away.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_walk_stops_once_the_consumer_has_gone() {
        let (tx, rx) = flightdeck::tracked::mpsc_channel("test", 4);
        drop(rx);

        let state = forward_off_the_runtime(tx, an_observation()).await;
        assert!(matches!(state, WalkState::Quit));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_walk_continues_while_the_consumer_is_listening() {
        let (tx, mut rx) = flightdeck::tracked::mpsc_channel("test", 4);

        let state = forward_off_the_runtime(tx, an_observation()).await;
        assert!(matches!(state, WalkState::Continue));

        let received = rx.next().await.expect("an item").expect("not an error");
        assert_eq!(received.rel_path, PathBuf::from("file.txt"));
    }

    /// Directories and other non-files produce nothing, which is not a reason
    /// to stop.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_entry_worth_nothing_does_not_stop_the_walk() {
        let (tx, _rx) = flightdeck::tracked::mpsc_channel("test", 4);

        let state = forward_off_the_runtime(tx, None).await;
        assert!(matches!(state, WalkState::Continue));
    }
}
