use crate::flightdeck::observation::Observation;
use crate::flightdeck::observation::Value;
use colored::*;
use tokio::task;

pub struct TerminalPipe {
    multi: indicatif::MultiProgress,
    level_filter: log::LevelFilter,
}

impl TerminalPipe {
    pub(crate) fn new(multi: indicatif::MultiProgress, level_filter: log::LevelFilter) -> Self {
        Self {
            multi,
            level_filter,
        }
    }

    pub(crate) async fn observe(&mut self, level: log::Level, obs: Observation) {
        if level > self.level_filter {
            return;
        }

        let msg: Option<String> = obs
            .data
            .iter()
            .filter_map(|data| {
                if data.key == *"state" {
                    if let Value::String(s) = data.value.clone() {
                        return Some(s);
                    }
                }
                None
            })
            .next();

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

        let multi = self.multi.clone();
        match task::spawn_blocking(move || multi.println(msg.to_string())).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => log::error!("could not write to the terminal via indactif: {}", e),
            Err(e) => log::error!("could not write to the terminal: {}", e),
        };
    }

    pub(crate) async fn finish(&mut self) {}
}
