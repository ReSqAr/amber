use crate::flightdeck::global::GLOBAL_LOGGER;
use crate::flightdeck::observation::Message;
use crate::flightdeck::observation::Observation;
use crate::flightdeck::progress_manager::{LayoutItemBuilderNode, ProgressManager};
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};

pub mod base;
pub mod global;
pub mod layout;
pub mod observation;
pub mod observer;
pub mod progress_manager;

pub trait Manager: Send + Sync {
    fn observe(&mut self, level: log::Level, obs: Observation);
    fn finish(&self);
}

pub struct NotifyOnDrop {
    pub tx: Sender<()>,
}

impl NotifyOnDrop {
    pub fn new(tx: Sender<()>) -> Self {
        Self { tx }
    }
}

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        let _ = self.tx.send(());
    }
}

pub fn notify_on_drop() -> (NotifyOnDrop, Receiver<()>) {
    let (tx, rx) = broadcast::channel::<()>(2);
    let guard = NotifyOnDrop::new(tx);
    (guard, rx)
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

    pub async fn run(&mut self, mut notify: Receiver<()>) {
        let mut rx_guard = GLOBAL_LOGGER.rx.lock().await;

        loop {
            select! {
                _ = notify.recv() => {
                    while let Ok(Message { level, observation }) = rx_guard.try_recv() {
                            for manager in self.manager.iter_mut() {
                                manager.observe(level, observation.clone());
                        }
                    }

                    for manager in self.manager.iter() {
                        manager.finish();
                    }
                    break;
                },

                msg = rx_guard.recv() => {
                    if let Some(Message { level, observation }) = msg {
                        for manager in self.manager.iter_mut() {
                            manager.observe(level, observation.clone());
                        }
                    } else {
                        for manager in self.manager.iter() {
                            manager.finish();
                        }
                        break;
                    }
                },
            }
        }
    }
}
