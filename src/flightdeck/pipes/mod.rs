use crate::flightdeck::observation::Observation;
use crate::flightdeck::pipes::file::FilePipe;
use crate::flightdeck::pipes::progress_bars::ProgressBarPipe;
use crate::flightdeck::pipes::terminal::TerminalPipe;

pub mod file;
pub mod progress_bars;
pub mod terminal;

#[derive(Default)]
pub struct Pipes {
    progress_bar: Option<ProgressBarPipe>,
    file: Option<FilePipe>,
    terminal: Option<TerminalPipe>,
}

impl Pipes {
    pub(crate) async fn observe(&mut self, level: log::Level, obs: Observation) {
        if let Some(progress_manager) = self.progress_bar.as_mut() {
            progress_manager.observe(level, obs.clone()).await;
        }
        if let Some(file_manager) = self.file.as_mut() {
            file_manager.observe(level, obs.clone()).await;
        }
        if let Some(terminal_manager) = self.terminal.as_mut() {
            terminal_manager.observe(level, obs.clone()).await;
        }
    }

    pub(crate) async fn finish(&mut self) {
        if let Some(progress_manager) = &self.progress_bar {
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
        }
    }
    pub(crate) fn set_file(self, file_manager: FilePipe) -> Self {
        Self {
            progress_bar: self.progress_bar,
            file: Some(file_manager),
            terminal: self.terminal,
        }
    }
    pub(crate) fn set_terminal(self, terminal_manager: TerminalPipe) -> Self {
        Self {
            progress_bar: self.progress_bar,
            file: self.file,
            terminal: Some(terminal_manager),
        }
    }
}
