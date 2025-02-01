use crate::flightdeck::observation::{Message, Observation};
use tokio::sync::{mpsc, Mutex};

pub(crate) struct GlobalLogger {
    pub(crate) tx: mpsc::UnboundedSender<Message>,
    pub(crate) rx: Mutex<mpsc::UnboundedReceiver<Message>>,
}

impl GlobalLogger {
    fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<Message>();
        Self { tx, rx: rx.into() }
    }
}

pub(crate) static GLOBAL_LOGGER: once_cell::sync::Lazy<GlobalLogger> =
    once_cell::sync::Lazy::new(GlobalLogger::new);

pub fn send(level: log::Level, observation: Observation) {
    if let Err(e) = GLOBAL_LOGGER.tx.send(Message { level, observation }) {
        log::error!("error while sending log message: {e}")
    }
}
