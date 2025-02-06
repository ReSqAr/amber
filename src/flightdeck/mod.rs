use crate::flightdeck::file_manager::FileManager;
use crate::flightdeck::global::GLOBAL_LOGGER;
use crate::flightdeck::observation::Message;
use crate::flightdeck::observation::Observation;
use crate::flightdeck::progress_manager::{LayoutItemBuilderNode, ProgressManager};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::sync::broadcast;

pub mod base;
pub mod file_manager;
pub mod global;
pub mod layout;
pub mod observation;
pub mod observer;
pub mod progress_manager;

#[derive(Default)]
struct Manager {
    progress_manager: Option<ProgressManager>,
    file_manager: Option<FileManager>,
}

impl Manager {
    pub(crate) async fn observe(&mut self, level: log::Level, obs: Observation) {
        if let Some(progress_manager) = self.progress_manager.as_mut() {
            progress_manager.observe(level, obs.clone()).await;
        }
        if let Some(file_manager) = self.file_manager.as_mut() {
            file_manager.observe(level, obs.clone()).await;
        }
    }

    pub(crate) async fn finish(&mut self) {
        if let Some(progress_manager) = &self.progress_manager {
            progress_manager.finish().await;
        }
        if let Some(file_manager) = self.file_manager.as_mut() {
            file_manager.finish().await;
        }
    }
}

impl Manager {
    pub(crate) fn set_progress(self, progress_manager: ProgressManager) -> Self {
        Self {
            progress_manager: Some(progress_manager),
            file_manager: self.file_manager,
        }
    }
    pub(crate) fn set_file(self, file_manager: FileManager) -> Self {
        Self {
            progress_manager: self.progress_manager,
            file_manager: Some(file_manager),
        }
    }
}

pub struct NotifyOnDrop {
    pub tx: broadcast::Sender<()>,
}

impl NotifyOnDrop {
    pub fn new(tx: broadcast::Sender<()>) -> Self {
        Self { tx }
    }
}

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        let _ = self.tx.send(());
    }
}

pub fn notify_on_drop() -> (NotifyOnDrop, broadcast::Receiver<()>) {
    let (tx, rx) = broadcast::channel::<()>(2);
    let guard = NotifyOnDrop::new(tx);
    (guard, rx)
}

#[derive(Default)]
pub struct FlightDeck {
    manager: Manager,
}

pub async fn flightdeck<E: From<tokio::task::JoinError>>(
    wrapped: impl std::future::Future<Output = Result<(), E>> + Sized,
    root_builders: impl IntoIterator<Item = LayoutItemBuilderNode> + Sized + Send + Sync + 'static,
    path: PathBuf,
    level_filter: log::LevelFilter,
) -> Result<(), E> {
    let (drop_to_notify, notify) = notify_on_drop();
    let join_handle = tokio::spawn(async move {
        FlightDeck::new()
            .with_progress(root_builders)
            .with_file(path, level_filter)
            .await
            .run(notify)
            .await;
    });

    wrapped.await?;

    drop(drop_to_notify);
    join_handle.await?;

    Ok(())
}

impl FlightDeck {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_progress<I>(self, root_builders: I) -> Self
    where
        I: IntoIterator<Item = LayoutItemBuilderNode>,
    {
        Self {
            manager: self
                .manager
                .set_progress(ProgressManager::new(root_builders)),
        }
    }

    pub async fn with_file(self, path: impl AsRef<Path>, level_filter: log::LevelFilter) -> Self {
        let path = path.as_ref();

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.unwrap_or_else(|_| {
                panic!("unable to create parent directory for {}", path.display())
            });
        }

        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .await
            .unwrap_or_else(|_| panic!("unable to open log file {}", path.display()));
        let writer = Box::new(file);
        Self {
            manager: self
                .manager
                .set_file(FileManager::new(writer, level_filter)),
        }
    }

    pub async fn run(&mut self, mut shutdown: broadcast::Receiver<()>) {
        let mut rx_guard = GLOBAL_LOGGER.rx.lock().await;

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    while let Ok(Message { level, observation }) = rx_guard.try_recv() {
                        self.manager.observe(level, observation.clone()).await;
                    }

                    self.manager.finish().await;
                    break;
                },

                msg = rx_guard.recv() => {
                    if let Some(Message { level, observation }) = msg {
                        self.manager.observe(level, observation.clone()).await;
                    } else {
                        self.manager.finish().await;
                        break;
                    }
                },
            }
        }
    }
}
