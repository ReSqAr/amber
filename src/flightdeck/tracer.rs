use crate::flightdeck::base::BaseObservable;
use crate::flightdeck::observer::Observer;
use std::time::Duration;
use std::time::Instant;

#[must_use]
pub struct Tracer {
    obs: Observer<BaseObservable>,
    elapsed: Duration,
    start: Option<Instant>,
}

impl Tracer {
    pub fn new_on(id: impl Into<String>) -> Self {
        Self {
            obs: Observer::with_id("trace", id.into()),
            start: Some(Instant::now()),
            elapsed: Default::default(),
        }
    }

    pub fn new_off(id: impl Into<String>) -> Self {
        Self {
            obs: Observer::with_id("trace", id.into()),
            start: None,
            elapsed: Default::default(),
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

    pub fn measure(self) -> Duration {
        let mut obs = self.obs;
        let mut elapsed = self.elapsed;
        if let Some(start) = self.start {
            elapsed += start.elapsed();
        }
        obs.observe_termination_ext(
            log::Level::Debug,
            "done",
            [("delay_ns".into(), elapsed.as_nanos() as u64)],
        );
        elapsed
    }
}
