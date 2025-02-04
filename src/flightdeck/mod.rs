use crate::flightdeck::global::GLOBAL_LOGGER;
use crate::flightdeck::observation::Message;
use crate::flightdeck::observation::Observation;
use crate::flightdeck::progress_manager::{LayoutItemBuilderNode, ProgressManager};
use tokio::sync::broadcast;

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
    pub tx: broadcast::Sender<()>,
}

impl NotifyOnDrop {
    pub fn new(tx: broadcast::Sender<()>) -> Self {
        Self { tx }
    }
}

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        let _ = self.tx.send(());
    }
}

pub fn notify_on_drop() -> (NotifyOnDrop, broadcast::Receiver<()>) {
    let (tx, rx) = broadcast::channel::<()>(2);
    let guard = NotifyOnDrop::new(tx);
    (guard, rx)
}

#[derive(Default)]
pub struct FlightDeck {
    manager: Vec<Box<dyn Manager>>,
}

pub async fn flightdeck<E: From<tokio::task::JoinError>>(
    wrapped: impl std::future::Future<Output = Result<(), E>> + Sized,
    root_builders: impl IntoIterator<Item = LayoutItemBuilderNode> + Sized + Send + Sync + 'static,
) -> Result<(), E> {
    let (drop_to_notify, notify) = notify_on_drop();
    let join_handle = tokio::spawn(async {
        FlightDeck::new()
            .with_progress(root_builders)
            .run(notify)
            .await;
    });

    wrapped.await?;

    drop(drop_to_notify);
    join_handle.await?;

    Ok(())
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

    pub async fn run(&mut self, mut notify: broadcast::Receiver<()>) {
        let mut rx_guard = GLOBAL_LOGGER.rx.lock().await;

        loop {
            tokio::select! {
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
