#[derive(Default, Clone)]
pub enum Output {
    #[default]
    Default,
    #[allow(dead_code)]
    Override(std::sync::Arc<std::sync::Mutex<Box<dyn std::io::Write + Send + Sync>>>),
}

impl Output {
    pub fn println(&self, s: String) {
        match &self {
            Output::Default => println!("{}", s),
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
