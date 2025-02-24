use crate::flightdeck::observation::{Message, Observation};
use tokio::sync::{Mutex, mpsc};

#[derive(Debug)]
pub enum Flow<T> {
    Data(T),
    Shutdown,
}

pub(crate) struct GlobalLogger {
    pub(crate) tx: mpsc::UnboundedSender<Flow<Message>>,
    pub(crate) rx: Mutex<mpsc::UnboundedReceiver<Flow<Message>>>,
}

impl GlobalLogger {
    fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<Flow<Message>>();
        Self { tx, rx: rx.into() }
    }
}

pub(crate) static GLOBAL_LOGGER: once_cell::sync::Lazy<GlobalLogger> =
    once_cell::sync::Lazy::new(GlobalLogger::new);

pub(crate) fn send(level: log::Level, observation: Observation) {
    if let Err(e) = GLOBAL_LOGGER
        .tx
        .send(Flow::Data(Message { level, observation }))
    {
        log::error!("error while sending log message: {e}")
    }
}

pub(crate) fn send_shutdown_signal() {
    if let Err(e) = GLOBAL_LOGGER.tx.send(Flow::Shutdown) {
        log::error!("error while sending shutdown message: {e}")
    }
}
