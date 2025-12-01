use crate::flightdeck::base::BaseObserver;
use futures::FutureExt;
use futures_core::future::BoxFuture;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinHandle;
use tokio::time;

pub trait Tracker: Send + Sync + Clone + 'static {
    fn new(name: impl Into<String>) -> Self;
    fn heartbeat(&mut self, count: u64, delay_ns: u64);
    fn on_drop(&mut self, count: u64, delay_ns: u64);
}

pub struct TrackedSenderInner<T: 'static + Send + Sync, ST: Tracker> {
    inner: mpsc::Sender<T>,
    #[allow(dead_code)]
    name: String,
    counter: Arc<AtomicU64>,
    delay_ns: Arc<AtomicU64>,
    heartbeat_handle: Option<JoinHandle<()>>,
    tracker: ST,
}

impl<T: 'static + Send + Sync, ST: Tracker> TrackedSenderInner<T, ST> {
    pub fn new(s: mpsc::Sender<T>, name: impl Into<String>, duration: Duration) -> Self {
        let counter = Arc::new(AtomicU64::new(0));
        let delay_ns = Arc::new(AtomicU64::new(0));
        let name = name.into();
        let tracker = ST::new(name.clone());

        let counter_clone = counter.clone();
        let delay_ns_clone = delay_ns.clone();
        let tracker_clone = tracker.clone();
        let heartbeat_handle = tokio::spawn(async move {
            let mut tracker = tracker_clone.clone();
            let mut interval = time::interval(duration);
            loop {
                interval.tick().await;
                tracker.heartbeat(
                    counter_clone.load(Ordering::Relaxed),
                    delay_ns_clone.load(Ordering::Relaxed),
                );
            }
        });

        Self {
            inner: s,
            name,
            counter,
            delay_ns,
            tracker,
            heartbeat_handle: Some(heartbeat_handle),
        }
    }

    pub fn send(&self, msg: T) -> BoxFuture<'_, Result<(), SendError<T>>> {
        async {
            let start_time = time::Instant::now();

            let result = self.inner.send(msg).await;

            let delay = start_time.elapsed();
            self.counter.fetch_add(1, Ordering::Relaxed);
            self.delay_ns
                .fetch_add(delay.as_nanos() as u64, Ordering::Relaxed);

            result
        }
        .boxed()
    }

    pub(crate) fn blocking_send(&self, msg: T) -> Result<(), SendError<T>> {
        let start_time = time::Instant::now();

        let result = self.inner.blocking_send(msg);

        let delay = start_time.elapsed();
        self.counter.fetch_add(1, Ordering::Relaxed);
        self.delay_ns
            .fetch_add(delay.as_nanos() as u64, Ordering::Relaxed);

        result
    }

    #[allow(dead_code)]
    pub(crate) fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[allow(dead_code)]
    pub(crate) fn max_capacity(&self) -> usize {
        self.inner.max_capacity()
    }

    #[allow(dead_code)]
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl<T: 'static + Send + Sync, ST: Tracker> Drop for TrackedSenderInner<T, ST> {
    fn drop(&mut self) {
        if let Some(handle) = self.heartbeat_handle.take() {
            self.tracker.on_drop(
                self.counter.load(Ordering::Relaxed),
                self.delay_ns.load(Ordering::Relaxed),
            );
            handle.abort();
        }
    }
}

pub struct TrackedSender<T: 'static + Send + Sync, ST: Tracker> {
    inner: Arc<TrackedSenderInner<T, ST>>,
}

impl<T: 'static + Send + Sync, ST: Tracker> Clone for TrackedSender<T, ST> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: 'static + Send + Sync> TrackedSender<T, Adapter> {
    pub fn new(sender: mpsc::Sender<T>, name: impl Into<String>) -> Self {
        Self::with(sender, name, Duration::from_secs(1))
    }
}

impl<T: 'static + Send + Sync, ST: Tracker> TrackedSender<T, ST> {
    pub fn with(sender: mpsc::Sender<T>, name: impl Into<String>, duration: Duration) -> Self {
        Self {
            inner: Arc::new(TrackedSenderInner::new(sender, name, duration)),
        }
    }

    pub fn send(&self, msg: T) -> BoxFuture<'_, Result<(), SendError<T>>> {
        self.inner.send(msg)
    }

    #[allow(dead_code)]
    pub(crate) fn blocking_send(&self, msg: T) -> Result<(), SendError<T>> {
        self.inner.blocking_send(msg)
    }

    #[allow(dead_code)]
    pub(crate) fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[allow(dead_code)]
    pub(crate) fn max_capacity(&self) -> usize {
        self.inner.max_capacity()
    }

    #[allow(dead_code)]
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

#[derive(Clone)]
pub struct Adapter {
    inner: BaseObserver,
}

impl Tracker for Adapter {
    fn new(name: impl Into<String>) -> Self {
        Adapter {
            inner: BaseObserver::with_id("sink", name),
        }
    }

    fn heartbeat(&mut self, count: u64, delay_ns: u64) {
        self.inner
            .observe_position_ext(log::Level::Debug, count, [("delay_ns".into(), delay_ns)]);
    }

    fn on_drop(&mut self, count: u64, delay_ns: u64) {
        self.inner.observe_termination_ext(
            log::Level::Debug,
            "completed",
            [("position".into(), count), ("delay_ns".into(), delay_ns)],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;
    use tokio::time::{Duration, Instant, sleep};

    /// A dummy tracker for testing that records heartbeat and drop events.
    #[derive(Clone, Debug)]
    struct DummyTracker {
        // Records each heartbeat as count.
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

        fn heartbeat(&mut self, count: u64, _delay_ns: u64) {
            self.heartbeats.lock().unwrap().push(count);
        }

        fn on_drop(&mut self, count: u64, _delay_ns: u64) {
            self.drops.lock().unwrap().push(count);
        }
    }

    // For convenience in tests, we expose a getter on TrackingSenderInner.
    impl<T: 'static + Send + Sync, ST: Tracker> TrackedSenderInner<T, ST> {
        pub fn get_send_count(&self) -> u64 {
            self.counter.load(Ordering::Relaxed)
        }
    }

    /// Helper function: assert that the asynchronous value eventually equals expected,
    /// using exponential backoff until timeout.
    async fn assert_eventually_eq<T, F, Fut>(mut actual: F, expected: T, timeout: Duration)
    where
        T: PartialEq + std::fmt::Debug,
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let start = Instant::now();
        let mut delay = Duration::from_millis(10);
        loop {
            let value = actual().await;
            if value == expected || start.elapsed() >= timeout {
                assert_eq!(value, expected, "Condition not met within timeout");
                return;
            }
            sleep(delay).await;
            delay = delay.checked_mul(2).unwrap_or(delay);
        }
    }

    /// Test that sending messages increments the counter and delivers messages.
    #[tokio::test]
    async fn test_tracking_sender_send_count() {
        let (tx, mut rx) = mpsc::channel::<i32>(10);

        let tracker: TrackedSender<_, DummyTracker> =
            TrackedSender::with(tx, "dummy", Duration::from_millis(100));
        tracker.send(1).await.unwrap();
        tracker.send(2).await.unwrap();
        tracker.send(3).await.unwrap();

        assert_eq!(tracker.inner.get_send_count(), 3);
        drop(tracker);

        let mut received = Vec::new();
        while let Some(msg) = rx.recv().await {
            received.push(msg);
        }
        assert_eq!(received, vec![1, 2, 3]);
    }

    /// Test that on_drop is eventually called with the final send count.
    #[tokio::test]
    async fn test_tracking_sender_on_drop() {
        let (tx, _rx) = mpsc::channel::<i32>(10);
        let sender: TrackedSender<_, DummyTracker> =
            TrackedSender::with(tx, "dummy_drop", Duration::from_millis(100));

        sender.send(10).await.unwrap();
        sender.send(20).await.unwrap();
        sender.send(30).await.unwrap();
        assert_eq!(sender.inner.get_send_count(), 3);

        let tracker = sender.inner.tracker.clone();

        drop(sender);

        assert_eventually_eq(
            || async { tracker.drops.lock().unwrap().clone() },
            vec![3],
            Duration::from_millis(100),
        )
        .await;
    }

    /// Test that the heartbeat loop eventually calls heartbeat.
    #[tokio::test]
    async fn test_tracking_sender_heartbeat() {
        let (tx, _rx) = mpsc::channel::<i32>(10);
        let sender: TrackedSender<_, DummyTracker> =
            TrackedSender::with(tx, "dummy_heartbeat", Duration::from_millis(50));
        let dummy_tracker = sender.inner.tracker.clone();
        assert_eventually_eq(
            || async { dummy_tracker.heartbeats.lock().unwrap().clone() },
            vec![0],
            Duration::from_millis(100),
        )
        .await;
    }
}
