use std::io::IsTerminal;

#[derive(Clone)]
pub enum Output {
    Raw,
    MultiProgressBar(indicatif::MultiProgress),
    #[allow(dead_code)]
    Override(std::sync::Arc<std::sync::Mutex<Box<dyn std::io::Write + Send + Sync>>>),
}

impl Output {
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
        match &self {
            Output::MultiProgressBar(m) => m
                .println(&s)
                .map_err(|e| eprintln!("could not write to the terminal: {}", e))
                .unwrap_or(()),
            Output::Raw => println!("{}", s),
            Output::Override(mutex) => {
                let mut writer = mutex.lock().unwrap();
                writer.write_all(format!("{}\n", s).as_bytes()).unwrap()
            }
        }
    }
}

pub enum OutputStream {
    MultiProgress(indicatif::MultiProgress),
    Output(Output),
}
