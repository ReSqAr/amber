use crate::flightdeck::tracked::stream::Trackable;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;

pub mod sender;
pub mod stream;

pub fn mpsc_channel<T: 'static + Send + Sync>(
    name: impl Into<String>,
    buffer_size: usize,
) -> (
    sender::TrackedSender<T, sender::Adapter>,
    stream::TrackedStream<impl Stream<Item = T>, stream::Adapter>,
) {
    let name = name.into();
    let (tx, rx) = mpsc::channel(buffer_size);
    (
        sender::TrackedSender::new(tx, name.clone()),
        ReceiverStream::new(rx).track(name),
    )
}
