use crate::flightdeck::observation::{Data, Observation, Value};
use crate::flightdeck::output::Output;
use owo_colors::OwoColorize;
use tokio::task;

pub struct TerminalPipe {
    level_filter: log::LevelFilter,
    out: Output,
    buffer: Vec<String>,
}

impl TerminalPipe {
    pub(crate) fn new(out: Output, level_filter: log::LevelFilter) -> Self {
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

        let with_color = matches!(self.out, Output::MultiProgressBar(_));

        let colored_state = if with_color {
            match level {
                log::Level::Error => state.red().to_string(),
                log::Level::Warn => state.yellow().to_string(),
                log::Level::Info => state.blue().to_string(),
                log::Level::Debug => state.dimmed().to_string(),
                log::Level::Trace => state.dimmed().to_string(),
            }
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
        if let Some(count) = lookup_u64_key(&obs.data, "count") {
            parts.push(format!("count={}", count));
        }

        self.buffer.push(parts.join(" "));
    }

    pub(crate) async fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        let data = self.buffer.join("\n");
        self.buffer.clear();

        let output = self.out.clone();
        match task::spawn_blocking(move || output.println(data)).await {
            Ok(()) => {}
            Err(e) => eprintln!("could not write to the terminal: {}", e),
        }
    }

    pub(crate) async fn finish(&mut self) {
        self.flush().await
    }
}

#[allow(clippy::collapsible_if)]
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

#[allow(clippy::collapsible_if)]
fn lookup_u64_key(data: &[Data], key: &str) -> Option<u64> {
    data.iter()
        .filter_map(|data| {
            if data.key == *key {
                if let Value::U64(s) = data.value.clone() {
                    return Some(s);
                }
            }
            None
        })
        .next()
}
