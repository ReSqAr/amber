use crate::flightdeck::global::GLOBAL_LOGGER;
use crate::flightdeck::layout::LayoutItemBuilder;
use crate::flightdeck::observation::{Message, Observation};
use crate::flightdeck::process_manager::ProgressManager;

pub trait Manager {
    fn observe(&mut self, level: log::Level, obs: Observation);
}

pub struct FlightDeck {
    manager: Vec<Box<dyn Manager + Send + Sync>>,
}

impl FlightDeck {
    pub fn new() -> Self {
        Self { manager: vec![] }
    }

    pub fn with_progress(
        &mut self,
        root_builders: Vec<Box<dyn LayoutItemBuilder + Send + Sync>>,
    ) -> &mut Self {
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
