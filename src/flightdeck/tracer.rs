use crate::flightdeck::base::BaseObservable;
use crate::flightdeck::observation::Value;
use crate::flightdeck::observer::Observer;
use chrono::SecondsFormat;
use std::time::Duration;
use std::time::Instant;

#[must_use]
pub struct Tracer {
    obs: Observer<BaseObservable>,
    elapsed: Duration,
    start: Option<Instant>,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl Tracer {
    pub fn new_on(id: impl Into<String>) -> Self {
        Self {
            obs: Observer::with_id("trace", id.into()),
            start: Some(Instant::now()),
            elapsed: Default::default(),
            created_at: chrono::Utc::now(),
        }
    }

    pub fn new_off(id: impl Into<String>) -> Self {
        Self {
            obs: Observer::with_id("trace", id.into()),
            start: None,
            elapsed: Default::default(),
            created_at: chrono::Utc::now(),
        }
    }

    pub fn off(&mut self) {
        if let Some(start) = self.start.take() {
            self.elapsed += start.elapsed();
        }
    }
    pub fn on(&mut self) {
        if self.start.is_none() {
            self.start = Some(Instant::now());
        }
    }

    #[allow(clippy::must_use_candidate)]
    pub fn measure(self) -> Duration {
        let mut obs = self.obs;
        let mut elapsed = self.elapsed;
        if let Some(start) = self.start {
            elapsed += start.elapsed();
        }
        obs.observe_termination_ext(
            log::Level::Debug,
            "done",
            [
                ("delay_ns".into(), Value::U64(elapsed.as_nanos() as u64)),
                (
                    "created_at".into(),
                    Value::String(
                        self.created_at
                            .to_rfc3339_opts(SecondsFormat::Millis, true)
                            .to_string(),
                    ),
                ),
            ],
        );
        elapsed
    }
}
