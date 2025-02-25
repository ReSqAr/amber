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

#[cfg(test)]
mod tests {
    use crate::flightdeck::observation::{Data, Observation, Value};
    use crate::flightdeck::output::{Output, OutputStream};
    use crate::flightdeck::pipes::Pipes;
    use crate::flightdeck::pipes::file::FilePipe;
    use crate::flightdeck::pipes::progress_bars::{LayoutItemBuilderNode, ProgressBarPipe};
    use crate::flightdeck::pipes::terminal::TerminalPipe;
    use chrono::Utc;
    use indicatif::MultiProgress;

    use std::sync::{Arc, Mutex};
    use tokio::io::AsyncWrite;

    struct MockAsyncWrite {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl MockAsyncWrite {
        fn new() -> Self {
            Self {
                buffer: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn get_buffer(&self) -> Arc<Mutex<Vec<u8>>> {
            self.buffer.clone()
        }
    }

    impl AsyncWrite for MockAsyncWrite {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
            if let Ok(mut buffer) = self.buffer.lock() {
                buffer.extend_from_slice(buf);
            }
            std::task::Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_file_pipe() {
        let mock_writer = MockAsyncWrite::new();
        let buffer = mock_writer.get_buffer();

        let mut file_pipe = FilePipe::new(Box::new(mock_writer), log::LevelFilter::Info);

        let observation = Observation {
            type_key: "test".to_string(),
            id: Some("123".to_string()),
            timestamp: Utc::now(),
            is_terminal: false,
            data: vec![
                Data {
                    key: "state".to_string(),
                    value: Value::String("running".to_string()),
                },
                Data {
                    key: "progress".to_string(),
                    value: Value::U64(50),
                },
            ],
        };

        file_pipe.observe(log::Level::Info, observation);

        file_pipe.flush().await;
        {
            let buffer_contents = buffer.lock().unwrap();
            assert!(!buffer_contents.is_empty());
        }
        file_pipe.finish().await;
    }

    // Custom Write implementation for testing
    struct TestWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl TestWriter {
        fn new() -> (Self, Arc<Mutex<Vec<u8>>>) {
            let buffer = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    buffer: buffer.clone(),
                },
                buffer,
            )
        }
    }

    impl std::io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if let Ok(mut buffer) = self.buffer.lock() {
                buffer.extend_from_slice(buf);
            }
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_terminal_pipe() {
        let (writer, _buffer) = TestWriter::new();
        let write_box: Box<dyn std::io::Write + Send + Sync> = Box::new(writer);
        let output = Output::Override(Arc::new(Mutex::new(write_box)));

        let mut terminal_pipe =
            TerminalPipe::new(OutputStream::Output(output), log::LevelFilter::Info);

        let observation = Observation {
            type_key: "test".to_string(),
            id: Some("123".to_string()),
            timestamp: Utc::now(),
            is_terminal: false,
            data: vec![Data {
                key: "state".to_string(),
                value: Value::String("running".to_string()),
            }],
        };

        terminal_pipe.observe(log::Level::Info, observation);

        terminal_pipe.flush().await;

        terminal_pipe.finish().await;
    }

    #[test]
    fn test_progress_bar_pipe() {
        let multi = MultiProgress::new();

        let builders: Vec<LayoutItemBuilderNode> = Vec::new();

        let _progress_pipe = ProgressBarPipe::new(multi, builders);
    }

    #[tokio::test]
    async fn test_pipes_integration() {
        let mut pipes = Pipes::default();

        let observation = Observation {
            type_key: "test".to_string(),
            id: Some("123".to_string()),
            timestamp: Utc::now(),
            is_terminal: false,
            data: vec![Data {
                key: "state".to_string(),
                value: Value::String("running".to_string()),
            }],
        };

        pipes.observe(log::Level::Info, observation.clone()).await;

        pipes.flush().await;

        pipes.finish().await;

        let (writer, _buffer) = TestWriter::new();
        let write_box: Box<dyn std::io::Write + Send + Sync> = Box::new(writer);
        let output = Output::Override(Arc::new(Mutex::new(write_box)));

        let terminal_pipe = TerminalPipe::new(OutputStream::Output(output), log::LevelFilter::Info);

        pipes = pipes.set_terminal(terminal_pipe);

        pipes.observe(log::Level::Info, observation).await;

        pipes.flush().await;
        pipes.finish().await;
    }
}
