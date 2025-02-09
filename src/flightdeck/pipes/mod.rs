use crate::flightdeck::observation::Observation;
use crate::flightdeck::pipes::file::FilePipe;
use crate::flightdeck::pipes::progress_bars::ProgressBarPipe;
use crate::flightdeck::pipes::terminal::TerminalPipe;

pub mod file;
pub mod progress_bars;
pub mod terminal;

const MAX_FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

#[derive(Default)]
pub struct Pipes {
    progress_bar: Option<ProgressBarPipe>,
    file: Option<FilePipe>,
    terminal: Option<TerminalPipe>,
    last_flush: Option<tokio::time::Instant>,
}

impl Pipes {
    pub(crate) async fn observe(&mut self, level: log::Level, obs: Observation) {
        if let Some(progress_manager) = self.progress_bar.as_mut() {
            progress_manager.observe(level, obs.clone());
        }
        if let Some(file_manager) = self.file.as_mut() {
            file_manager.observe(level, obs.clone());
        }
        if let Some(terminal_manager) = self.terminal.as_mut() {
            terminal_manager.observe(level, obs.clone());
        }

        let needs_flush = self
            .last_flush
            .is_none_or(|t| t.elapsed() >= MAX_FLUSH_INTERVAL);
        if needs_flush {
            self.flush().await;
        }
    }

    pub(crate) async fn flush(&mut self) {
        if let Some(progress_manager) = self.progress_bar.as_mut() {
            progress_manager.flush().await;
        }
        if let Some(file_manager) = self.file.as_mut() {
            file_manager.flush().await;
        }
        if let Some(terminal_manager) = self.terminal.as_mut() {
            terminal_manager.flush().await;
        }
        self.last_flush = Some(tokio::time::Instant::now());
    }

    pub(crate) async fn finish(&mut self) {
        if let Some(progress_manager) = self.progress_bar.as_mut() {
            progress_manager.finish().await;
        }
        if let Some(file_manager) = self.file.as_mut() {
            file_manager.finish().await;
        }
        if let Some(terminal_manager) = self.terminal.as_mut() {
            terminal_manager.finish().await;
        }
    }
}

impl Pipes {
    pub(crate) fn set_progress(self, progress_manager: ProgressBarPipe) -> Self {
        Self {
            progress_bar: Some(progress_manager),
            file: self.file,
            terminal: self.terminal,
            last_flush: self.last_flush,
        }
    }

    pub(crate) fn set_file(self, file_manager: FilePipe) -> Self {
        Self {
            progress_bar: self.progress_bar,
            file: Some(file_manager),
            terminal: self.terminal,
            last_flush: self.last_flush,
        }
    }

    pub(crate) fn set_terminal(self, terminal_manager: TerminalPipe) -> Self {
        Self {
            progress_bar: self.progress_bar,
            file: self.file,
            terminal: Some(terminal_manager),
            last_flush: self.last_flush,
        }
    }
}
