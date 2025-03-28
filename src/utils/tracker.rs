use crate::flightdeck::base::BaseObserver;
use futures::stream::Stream;
use futures::task::{Context, Poll};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

pub trait Tracker: Send + Sync + Clone + Unpin + 'static {
    fn new(name: &str) -> Self;
    fn heartbeat(&mut self, position: u64);
    fn on_drop(&mut self, position: u64);
}

pub struct TrackingStream<S, T: Tracker> {
    inner: S,
    #[allow(dead_code)]
    name: String,
    counter: Arc<AtomicU64>,
    heartbeat_handle: Option<JoinHandle<()>>,
    tracker: T,
}

impl<S, T> Stream for TrackingStream<S, T>
where
    S: Stream + Unpin,
    T: Tracker,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = Pin::new(&mut self.inner).poll_next(cx);
        if let Poll::Ready(Some(_)) = res {
            self.counter.fetch_add(1, Ordering::Relaxed);
        }
        res
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<S, T> Drop for TrackingStream<S, T>
where
    T: Tracker,
{
    fn drop(&mut self) {
        if let Some(handle) = self.heartbeat_handle.take() {
            self.tracker.on_drop(self.counter.load(Ordering::Relaxed));
            handle.abort();
        }
    }
}

#[derive(Clone)]
pub struct FlightDeckAdapter {
    inner: BaseObserver,
}

impl Tracker for FlightDeckAdapter {
    fn new(name: &str) -> Self {
        FlightDeckAdapter {
            inner: BaseObserver::with_id("stream", name),
        }
    }

    fn heartbeat(&mut self, position: u64) {
        self.inner.observe_position(log::Level::Debug, position);
    }

    fn on_drop(&mut self, position: u64) {
        self.inner.observe_termination_ext(
            log::Level::Debug,
            "stream completed",
            [("position".into(), position)],
        );
    }
}

/// Extension trait to add the `.track()` method.
pub trait Trackable: Stream + Sized {
    /// Wraps this stream in a tracking wrapper that logs periodic heartbeat messages.
    fn track(self, name: &str) -> TrackingStream<Self, FlightDeckAdapter>;
    fn track_with<O: Tracker>(self, name: &str, interval: Duration) -> TrackingStream<Self, O>;
}

impl<S> Trackable for S
where
    S: Stream + Unpin + Send + 'static,
{
    fn track(self, name: &str) -> TrackingStream<Self, FlightDeckAdapter> {
        self.track_with::<FlightDeckAdapter>(name, Duration::from_secs(1))
    }

    fn track_with<O: Tracker>(self, name: &str, duration: Duration) -> TrackingStream<Self, O> {
        let counter = Arc::new(AtomicU64::new(0));
        let obs = O::new(name);

        let counter_clone = counter.clone();
        let mut obs_clone = obs.clone();
        let heartbeat_handle = tokio::spawn(async move {
            let mut interval = time::interval(duration);
            loop {
                interval.tick().await;
                obs_clone.heartbeat(counter_clone.load(Ordering::Relaxed));
            }
        });

        TrackingStream {
            inner: self,
            name: name.to_string(),
            counter,
            tracker: obs,
            heartbeat_handle: Some(heartbeat_handle),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, stream};
    use std::sync::{Arc, Mutex};
    use tokio::time::{Instant, sleep};

    /// A dummy Tracker implementation for testing.
    #[derive(Clone, Debug)]
    struct DummyTracker {
        heartbeats: Arc<Mutex<Vec<u64>>>,
        drops: Arc<Mutex<Vec<u64>>>,
    }

    impl DummyTracker {
        fn new_instance() -> Self {
            DummyTracker {
                heartbeats: Arc::new(Mutex::new(Vec::new())),
                drops: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl Tracker for DummyTracker {
        fn new(_name: &str) -> Self {
            Self::new_instance()
        }
        fn heartbeat(&mut self, position: u64) {
            self.heartbeats.lock().unwrap().push(position);
        }
        fn on_drop(&mut self, position: u64) {
            self.drops.lock().unwrap().push(position);
        }
    }

    impl<S, O: Tracker> TrackingStream<S, O> {
        pub fn get_counter(&self) -> Arc<AtomicU64> {
            self.counter.clone()
        }
        pub fn get_tracker(&self) -> O {
            self.tracker.clone()
        }
    }

    async fn assert_eventually_eq<T, F, Fut>(mut actual: F, expected: T, timeout: Duration)
    where
        T: PartialEq + std::fmt::Debug,
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let start = Instant::now();
        let mut delay = Duration::from_millis(1);
        loop {
            let value = actual().await;
            if value == expected || start.elapsed() >= timeout {
                assert_eq!(value, expected);
                return;
            }
            sleep(delay).await;
            delay = delay.checked_mul(2).unwrap_or(delay);
        }
    }

    /// Test that polling the stream increments the counter.
    #[tokio::test]
    async fn test_tracking_stream_polling() {
        let original_stream = stream::iter(vec![1, 2, 3]);
        let tracked_stream =
            original_stream.track_with::<DummyTracker>("dummy_poll", Duration::from_secs(1));
        let counter = tracked_stream.get_counter();
        let collected: Vec<_> = tracked_stream.collect().await;
        assert_eq!(collected, vec![1, 2, 3]);
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }

    /// Test that the dummy observer receives the on_drop call with the final counter value.
    #[tokio::test]
    async fn test_tracking_stream_on_drop() {
        let original_stream = stream::iter(vec![10, 20]);
        let tracked_stream =
            original_stream.track_with::<DummyTracker>("dummy_drop", Duration::from_secs(1));
        let tracker = tracked_stream.get_tracker();
        let _collected: Vec<_> = tracked_stream.collect().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let drops = tracker.drops.lock().unwrap();
        assert_eq!(drops.first().copied().unwrap_or(0), 2);
    }

    /// Test that heartbeat is called shortly after starting the tracking.
    #[tokio::test]
    async fn test_tracking_stream_heartbeat() {
        let original_stream = stream::empty::<i32>();
        let tracked_stream = original_stream
            .track_with::<DummyTracker>("dummy_heartbeat", Duration::from_secs(3600));
        let tracker = tracked_stream.get_tracker();

        assert_eventually_eq(
            || async { tracker.heartbeats.lock().unwrap().clone() },
            vec![0],
            Duration::from_millis(100),
        )
        .await;
    }
}
