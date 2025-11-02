use crate::flightdeck::base::BaseObserver;
use futures::stream::Stream;
use futures::task::{Context, Poll};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

pub trait Tracker: Send + Sync + Clone + Unpin + 'static {
    fn new(name: impl Into<String>) -> Self;
    fn heartbeat(&mut self, count: u64);
    fn on_drop(&mut self, count: u64);
}

pub struct TrackedStream<S, T: Tracker> {
    inner: S,
    #[allow(dead_code)]
    name: String,
    counter: Arc<AtomicU64>,
    heartbeat_handle: Option<JoinHandle<()>>,
    tracker: T,
}

impl<S, T: Tracker> TrackedStream<S, T> {
    fn new(s: S, name: String, duration: Duration) -> Self {
        let counter = Arc::new(AtomicU64::new(0));
        let tracker = T::new(name.clone());

        let counter_clone = counter.clone();
        let mut tracker_clone = tracker.clone();
        let heartbeat_handle = tokio::spawn(async move {
            let mut interval = time::interval(duration);
            loop {
                interval.tick().await;
                tracker_clone.heartbeat(counter_clone.load(Ordering::Relaxed));
            }
        });

        Self {
            inner: s,
            name,
            counter,
            tracker,
            heartbeat_handle: Some(heartbeat_handle),
        }
    }
}

impl<S, T> Stream for TrackedStream<S, T>
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

impl<S, T> Drop for TrackedStream<S, T>
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
pub struct Adapter {
    inner: BaseObserver,
}

impl Tracker for Adapter {
    fn new(name: impl Into<String>) -> Self {
        Adapter {
            inner: BaseObserver::with_id("stream", name),
        }
    }

    fn heartbeat(&mut self, count: u64) {
        self.inner.observe_position(log::Level::Debug, count);
    }

    fn on_drop(&mut self, count: u64) {
        self.inner.observe_termination_ext(
            log::Level::Debug,
            "completed",
            [("position".into(), count)],
        );
    }
}

/// Extension trait to add the `.track()` method.
pub trait Trackable: Stream + Sized {
    /// Wraps this stream in a tracking wrapper that logs periodic heartbeat messages.
    fn track(self, name: impl Into<String>) -> TrackedStream<Self, Adapter>;
    fn track_with<O: Tracker>(
        self,
        name: impl Into<String>,
        interval: Duration,
    ) -> TrackedStream<Self, O>;
}

impl<S> Trackable for S
where
    S: Stream + Unpin + Send + 'static,
{
    fn track(self, name: impl Into<String>) -> TrackedStream<Self, Adapter> {
        self.track_with::<Adapter>(name.into(), Duration::from_secs(1))
    }

    fn track_with<T: Tracker>(
        self,
        name: impl Into<String>,
        duration: Duration,
    ) -> TrackedStream<Self, T> {
        TrackedStream::new(self, name.into(), duration)
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
        fn new(_name: impl Into<String>) -> Self {
            Self::new_instance()
        }
        fn heartbeat(&mut self, count: u64) {
            self.heartbeats.lock().unwrap().push(count);
        }
        fn on_drop(&mut self, count: u64) {
            self.drops.lock().unwrap().push(count);
        }
    }

    impl<S, O: Tracker> TrackedStream<S, O> {
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
        assert_eventually_eq(
            || async { tracker.drops.lock().unwrap().clone() },
            vec![2],
            Duration::from_millis(100),
        )
        .await;
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

    #[tokio::test]
    async fn test_tracking_stream_size_hint() {
        let inner = stream::iter(vec![1, 2, 3, 4]);
        let tracked = inner.track_with::<DummyTracker>("dummy_size_hint", Duration::from_secs(1));
        let size_hint = tracked.size_hint();
        assert_eq!(size_hint, (4, Some(4)));
    }
}
