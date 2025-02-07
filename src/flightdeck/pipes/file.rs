use crate::flightdeck::observation::Observation;
use crate::flightdeck::observation::Value;
use chrono::SecondsFormat;
use sqlx::types::JsonValue;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub struct FilePipe {
    writer: Box<dyn AsyncWrite + Send + Sync + Unpin>,
    level_filter: log::LevelFilter,
    buffer: Vec<String>,
}

impl FilePipe {
    pub(crate) fn new(
        writer: Box<dyn AsyncWrite + Send + Sync + Unpin>,
        level_filter: log::LevelFilter,
    ) -> Self {
        Self {
            writer,
            level_filter,
            buffer: vec![],
        }
    }
}

impl FilePipe {
    pub(crate) fn observe(&mut self, level: log::Level, obs: Observation) {
        if level > self.level_filter {
            return;
        }

        let mut log_object = serde_json::Map::new();

        log_object.insert("level".to_string(), JsonValue::String(level.to_string()));

        if let Some(id_value) = obs.id.map(JsonValue::String) {
            log_object.insert("id".to_string(), id_value);
        }

        let timestamp = obs.timestamp.to_rfc3339_opts(SecondsFormat::Millis, true);
        log_object.insert("timestamp".to_string(), JsonValue::String(timestamp));

        for data in obs.data {
            let value = match data.value {
                Value::String(s) => JsonValue::String(s),
                Value::U64(u) => JsonValue::Number(serde_json::Number::from(u)),
            };
            log_object.insert(data.key, value);
        }

        // serialise
        let json_line = match serde_json::to_string(&log_object) {
            Ok(s) => s,
            Err(e) => {
                log::error!("flightdeck error: unable to serialise log message: {}", e);
                return;
            }
        };

        self.buffer.push(json_line + "\n");
    }

    pub(crate) async fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        let data = self.buffer.concat();
        self.buffer.clear();

        if let Err(e) = self.writer.write_all(data.as_bytes()).await {
            log::error!("flightdeck error: unable to write logs: {}", e);
        }
        if let Err(e) = self.writer.flush().await {
            log::error!("flightdeck error: unable to flush logs: {}", e);
        }
    }

    pub(crate) async fn finish(&mut self) {
        self.flush().await
    }
}
