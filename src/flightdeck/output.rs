use std::io::IsTerminal;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

/// Set once the far end of stdout has gone away.
///
/// A reader that exits early - `amber status | head` - closes the pipe, and
/// every subsequent write fails with `BrokenPipe`. That is an ordinary way for
/// a command to be used, not an error, so the first one silences output and the
/// rest are dropped without further syscalls.
static STDOUT_CLOSED: AtomicBool = AtomicBool::new(false);

/// Reports whether output has been silenced because stdout was closed.
pub fn stdout_closed() -> bool {
    STDOUT_CLOSED.load(Ordering::Relaxed)
}

fn handle_write_error(e: &std::io::Error, what: &str) {
    if e.kind() == std::io::ErrorKind::BrokenPipe {
        STDOUT_CLOSED.store(true, Ordering::Relaxed);
    } else {
        eprintln!("could not write to {what}: {e}");
    }
}

#[derive(Clone)]
pub enum Output {
    Raw,
    MultiProgressBar(indicatif::MultiProgress),
    #[allow(dead_code)]
    Override(std::sync::Arc<std::sync::Mutex<Box<dyn std::io::Write + Send + Sync>>>),
}

impl Output {
    #[must_use]
    pub fn multi_progress_bar(&self) -> Option<indicatif::MultiProgress> {
        match self {
            Output::Raw => None,
            Output::MultiProgressBar(m) => Some(m.clone()),
            Output::Override(_) => None,
        }
    }
}

impl Default for Output {
    fn default() -> Self {
        if std::io::stdout().is_terminal() {
            let draw_target = indicatif::ProgressDrawTarget::stdout_with_hz(10);
            Self::MultiProgressBar(indicatif::MultiProgress::with_draw_target(draw_target))
        } else {
            Self::Raw
        }
    }
}

impl Output {
    pub fn println(&self, s: String) {
        if stdout_closed() {
            return;
        }

        match &self {
            Output::MultiProgressBar(m) => {
                if let Err(e) = m.println(&s) {
                    handle_write_error(&e, "the terminal");
                }
            }
            Output::Raw => {
                // Not `println!`, which panics when the pipe is closed - and
                // does so on a worker thread, leaving the process to report
                // success while printing a panic to stderr.
                let stdout = std::io::stdout();
                let mut stdout = stdout.lock();
                if let Err(e) = writeln!(stdout, "{s}") {
                    handle_write_error(&e, "stdout");
                }
            }
            Output::Override(mutex) => match mutex.lock() {
                Ok(mut writer) => {
                    if let Err(e) = writer.write_all(format!("{s}\n").as_bytes()) {
                        handle_write_error(&e, "the output");
                    }
                }
                Err(e) => eprintln!("could not write to the output: {e}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A writer whose reader has gone away.
    struct BrokenPipeWriter;

    impl std::io::Write for BrokenPipeWriter {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "broken pipe",
            ))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// A writer that fails for a reason other than the reader leaving.
    struct FailingWriter;

    impl std::io::Write for FailingWriter {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("disk on fire"))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn output_over(w: Box<dyn std::io::Write + Send + Sync>) -> Output {
        Output::Override(Arc::new(Mutex::new(w)))
    }

    /// `amber status | head` closes the pipe while the command is still
    /// producing lines. Writing to it must not take the process down.
    #[test]
    fn a_closed_pipe_does_not_panic() {
        let output = output_over(Box::new(BrokenPipeWriter));

        output.println("first".to_string());
        output.println("second".to_string());

        assert!(stdout_closed(), "a broken pipe should silence output");

        // Reset so the flag does not leak into other tests in this binary.
        STDOUT_CLOSED.store(false, Ordering::Relaxed);
    }

    #[test]
    fn other_write_errors_do_not_silence_output() {
        let output = output_over(Box::new(FailingWriter));

        output.println("first".to_string());

        assert!(!stdout_closed(), "only a broken pipe should silence output");
    }

    #[test]
    fn writing_succeeds_through_an_override() {
        #[derive(Clone)]
        struct Collect(Arc<Mutex<Vec<u8>>>);

        impl std::io::Write for Collect {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().expect("lock").extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let sink = Arc::new(Mutex::new(Vec::new()));
        let output = output_over(Box::new(Collect(sink.clone())));

        output.println("hello".to_string());

        let written = sink.lock().expect("lock").clone();
        assert_eq!(String::from_utf8(written).expect("utf8"), "hello\n");
    }
}
