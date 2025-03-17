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

        let state = match lookup_key(&obs.data, "state") {
            Some(s) => s,
            None => return,
        };

        let with_color = matches!(self.out, OutputStream::MultiProgress(_));

        let colored_state = if with_color {
            match level {
                log::Level::Error => state.red(),
                log::Level::Warn => state.yellow(),
                log::Level::Info => state.blue(),
                log::Level::Debug => state.dimmed(),
                log::Level::Trace => state.dimmed(),
            }
            .to_string()
        } else {
            state.to_string()
        };

        let mut parts = vec![colored_state];

        if let Some(id) = obs.id {
            parts.push(if with_color {
                id.bold().to_string()
            } else {
                id
            });
        }

        if let Some(detail) = lookup_key(&obs.data, "detail") {
            parts.push(detail.to_string());
        }

        self.buffer.push(parts.join(" "));
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
                        eprintln!("could not write to the terminal via indactif: {}", e)
                    }
                    Err(e) => eprintln!("could not write to the terminal: {}", e),
                }
            }
            OutputStream::Output(output) => {
                let output = output.clone();
                match task::spawn_blocking(move || output.println(data.to_string())).await {
                    Ok(()) => {}
                    Err(e) => eprintln!("could not write to the terminal: {}", e),
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
