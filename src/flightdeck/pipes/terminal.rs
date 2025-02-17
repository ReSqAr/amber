use crate::flightdeck::observation::{Data, Observation, Value};
use crate::flightdeck::output::OutputStream;
use colored::*;
use tokio::task;

pub struct TerminalPipe {
    level_filter: log::LevelFilter,
    out: OutputStream,
    buffer: Vec<String>,
}

impl TerminalPipe {
    pub(crate) fn new(out: OutputStream, level_filter: log::LevelFilter) -> Self {
        Self {
            out,
            level_filter,
            buffer: vec![],
        }
    }

    pub(crate) fn observe(&mut self, level: log::Level, obs: Observation) {
        if level > self.level_filter {
            return;
        }

        let msg = lookup_key(&obs.data, "state");
        let msg = match msg {
            Some(msg) => msg,
            None => return,
        };

        let msg = match level {
            log::Level::Error => msg.red(),
            log::Level::Warn => msg.yellow(),
            log::Level::Info => msg.blue(),
            log::Level::Debug => msg.dimmed(),
            log::Level::Trace => msg.dimmed(),
        };

        let msg = match obs.id {
            None => msg,
            Some(id) => format!("{msg} {}", id.bold()).normal(),
        };

        let msg = if let Some(detail) = lookup_key(&obs.data, "detail") {
            format!("{msg} {}", detail.normal()).normal()
        } else {
            msg
        };

        self.buffer.push(msg.to_string())
    }

    pub(crate) async fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        let data = self.buffer.join("\n");
        self.buffer.clear();

        match &self.out {
            OutputStream::MultiProgress(multi) => {
                let multi = multi.clone();
                match task::spawn_blocking(move || multi.println(&data)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        log::error!("could not write to the terminal via indactif: {}", e)
                    }
                    Err(e) => log::error!("could not write to the terminal: {}", e),
                }
            }
            OutputStream::Output(output) => {
                let output = output.clone();
                match task::spawn_blocking(move || output.println(data.to_string())).await {
                    Ok(()) => {}
                    Err(e) => log::error!("could not write to the terminal: {}", e),
                }
            }
        }
    }

    pub(crate) async fn finish(&mut self) {
        self.flush().await
    }
}

fn lookup_key(data: &[Data], key: &str) -> Option<String> {
    data.iter()
        .filter_map(|data| {
            if data.key == *key {
                if let Value::String(s) = data.value.clone() {
                    return Some(s);
                }
            }
            None
        })
        .next()
}
