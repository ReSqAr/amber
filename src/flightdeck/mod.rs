use crate::flightdeck::global::{Flow, GLOBAL_LOGGER, send_shutdown_signal};
use crate::flightdeck::observation::Message;
use output::Output;
use pipes::Pipes;
use pipes::file::FilePipe;
use pipes::progress_bars::{LayoutItemBuilderNode, ProgressBarPipe};
use pipes::terminal::TerminalPipe;
use std::path::{Path, PathBuf};
use tokio::fs;

pub mod base;
pub mod global;
pub mod layout;
pub mod observation;
pub mod observer;
pub mod output;
pub mod pipes;

const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

#[derive(Default)]
pub struct FlightDeck {
    manager: Pipes,
}

const DEFAULT_TERMINAL_FILTER: log::LevelFilter = log::LevelFilter::Info;
const DEFAULT_FILE_FILTER: log::LevelFilter = log::LevelFilter::Debug;

pub async fn flightdeck<E: From<tokio::task::JoinError>>(
    wrapped: impl Future<Output = Result<(), E>> + Sized,
    root_builders: impl IntoIterator<Item = LayoutItemBuilderNode> + Sized + Send + Sync + 'static,
    path: impl Into<Option<PathBuf>>,
    file_level_filter: Option<log::LevelFilter>,
    terminal_level_filter: Option<log::LevelFilter>,
    output: Output,
) -> Result<(), E> {
    let path = path.into();
    let join_handle = tokio::spawn(async move {
        let flightdeck = FlightDeck::new();
        let flightdeck = if let Some(multi) = output.multi_progress_bar() {
            flightdeck.with_progress(multi, root_builders)
        } else {
            flightdeck
        };
        let flightdeck = flightdeck.with_terminal(
            output,
            terminal_level_filter.unwrap_or(DEFAULT_TERMINAL_FILTER),
        );
        let mut flightdeck = match path {
            None => flightdeck,
            Some(path) => {
                flightdeck
                    .with_file(path, file_level_filter.unwrap_or(DEFAULT_FILE_FILTER))
                    .await
            }
        };

        flightdeck.run().await;
    });

    let result = wrapped.await;

    send_shutdown_signal();
    join_handle.await?;

    result
}

impl FlightDeck {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_progress<I>(self, multi: indicatif::MultiProgress, root_builders: I) -> Self
    where
        I: IntoIterator<Item = LayoutItemBuilderNode>,
    {
        Self {
            manager: self
                .manager
                .set_progress(ProgressBarPipe::new(multi, root_builders)),
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
            manager: self.manager.set_file(FilePipe::new(writer, level_filter)),
        }
    }

    pub fn with_terminal(self, output: Output, level_filter: log::LevelFilter) -> Self {
        Self {
            manager: self
                .manager
                .set_terminal(TerminalPipe::new(output, level_filter)),
        }
    }

    pub async fn run(&mut self) {
        let mut rx_guard = GLOBAL_LOGGER.rx.lock().await;
        let mut interval = tokio::time::interval(FLUSH_INTERVAL);

        loop {
            tokio::select! {
                msg = rx_guard.recv() => {
                    match msg {
                        Some(Flow::Data(Message { level, observation })) => {
                            self.manager.observe(level, observation.clone()).await;
                        },
                        Some(Flow::Shutdown) => {
                            while let Ok(msg) = rx_guard.try_recv() {
                                if let Flow::Data(Message { level, observation }) = msg {
                                    self.manager.observe(level, observation.clone()).await;
                                }
                            }
                            self.manager.flush().await;
                            self.manager.finish().await;
                            break;
                        }
                        None => {
                            self.manager.finish().await;
                            break;
                        }
                    };
                },

                _ = interval.tick() => {
                    self.manager.flush().await;
                },
            }
        }
    }
}
