use crate::flightdeck::global::GLOBAL_LOGGER;
use crate::flightdeck::observation::Message;
use crate::flightdeck::observation::Observation;
use crate::flightdeck::progress_manager::{LayoutItemBuilderNode, ProgressManager};

pub mod base;
pub mod global;
pub mod layout;
pub mod observation;
pub mod observer;
pub mod progress_manager;

pub trait Manager: Send + Sync {
    fn observe(&mut self, level: log::Level, obs: Observation);
}

#[derive(Default)]
pub struct FlightDeck {
    manager: Vec<Box<dyn Manager>>,
}

impl FlightDeck {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_progress<I>(&mut self, root_builders: I) -> &mut Self
    where
        I: IntoIterator<Item = LayoutItemBuilderNode>,
    {
        self.manager
            .push(Box::new(ProgressManager::new(root_builders)));
        self
    }

    pub async fn run(&mut self) {
        while let Some(message) = GLOBAL_LOGGER.rx.lock().await.recv().await {
            let Message { level, observation } = message;
            for manager in &mut self.manager {
                manager.observe(level, observation.clone());
            }
        }
    }
}
