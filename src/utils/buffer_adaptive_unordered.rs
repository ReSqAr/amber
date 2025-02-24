use crate::flightdeck::base::BaseObservable;
use crate::flightdeck::observation::Value;
use crate::flightdeck::observer::Observer;
use core::fmt;
use core::pin::Pin;
use futures::stream::{Fuse, FuturesUnordered, StreamExt};
use futures_core::future::Future;
use futures_core::stream::{FusedStream, Stream};
use futures_core::task::{Context, Poll};
use pin_project_lite::pin_project;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// The adjustment action taken at the last decision.
#[derive(Debug, Copy, Clone)]
enum AdjustmentAction {
    Increase,
    Decrease,
}

/// Tuning constants:
/// - RAMP_UP_MULTIPLIER and RAMP_DOWN_MULTIPLIER are used for multiplicative adjustments.
/// - WINDOW_DURATION is the length of the sliding window used to compute throughput.
/// - IDLE_TIMEOUT forces a decrease when no completions occur.
/// - MIN_CONCURRENCY and MAX_CONCURRENCY cap the allowed concurrency.
const RAMP_UP_MULTIPLIER: f64 = std::f64::consts::SQRT_2;
const RAMP_DOWN_MULTIPLIER: f64 = std::f64::consts::FRAC_1_SQRT_2;
const WINDOW_DURATION: Duration = Duration::from_secs(10);
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const MIN_CONCURRENCY: usize = 1;
const MAX_CONCURRENCY: usize = 102400;

/// An adaptive concurrency controller that uses sliding windows.
/// In this version we also take into account the “task size” (e.g. number of bytes processed)
/// so that the controller maximizes throughput (bytes/second) while keeping the latency (per byte)
/// under control.  After each window the new performance is compared to the baseline (from the previous
/// window).  We compute a performance index by taking the ratio:
///
/// ```
///   (current_bytes_throughput / previous_bytes_throughput)
///   ---------------------------------------------------------
///   (current_normalized_latency / previous_normalized_latency)
/// ```
///
/// If this index is greater than 1 then we consider that the system improved overall, and we increase
/// the concurrency limit. Otherwise we decrease it. (Multiplicative adjustments via the ramp multipliers
/// help keep the controller stable.)
struct AdaptiveConcurrency {
    /// Current concurrency limit.
    max: usize,
    /// Time of the last adjustment.
    last_adjustment: Instant,
    /// Sliding window of completion event timestamps (only events after the last adjustment).
    completion_events: VecDeque<Instant>,
    /// Baseline throughput (in bytes/second) measured in the window before the last adjustment.
    pre_adjustment_bytes_throughput: Option<f64>,
    /// Baseline normalized latency (seconds per byte) measured in the window before the last adjustment.
    pre_adjustment_normalized_latency: Option<f64>,
    /// Accumulated latency (in seconds) for completions in the current window.
    total_latency: f64,
    /// Count of completions in the current window (for latency averaging).
    latency_count: usize,
    /// Total task size processed in the current window (e.g. total bytes processed).
    total_task_size: f64,
    /// Observer for logging state (adjust as needed for your project).
    obs: Observer<BaseObservable>,
}

impl AdaptiveConcurrency {
    /// Create a new controller starting at the given initial concurrency.
    pub fn new(initial: usize) -> Self {
        Self {
            max: initial.max(MIN_CONCURRENCY),
            last_adjustment: Instant::now(),
            completion_events: VecDeque::new(),
            pre_adjustment_bytes_throughput: None,
            pre_adjustment_normalized_latency: None,
            total_latency: 0.0,
            latency_count: 0,
            total_task_size: 0.0,
            obs: Observer::without_id("auto_concurrency"),
        }
    }

    /// Record a completed task along with its observed latency and the size of the task.
    pub fn record_completion(&mut self, latency: Duration, task_size: f64) {
        // Record the completion event timestamp.
        self.completion_events.push_back(Instant::now());
        self.total_latency += latency.as_secs_f64();
        self.latency_count += 1;
        self.total_task_size += task_size;
    }

    /// Evaluate the sliding window and, if enough time has passed, adjust concurrency.
    pub fn tick(&mut self) {
        let now = Instant::now();
        let elapsed_since_adjustment = now.duration_since(self.last_adjustment);

        // Remove any events older than the defined window.
        while let Some(&ts) = self.completion_events.front() {
            if now.duration_since(ts) > WINDOW_DURATION {
                self.completion_events.pop_front();
            } else {
                break;
            }
        }

        // If the elapsed time since the last adjustment exceeds our window duration,
        // compute the current window’s metrics.
        if elapsed_since_adjustment >= WINDOW_DURATION {
            let window_secs = WINDOW_DURATION.as_secs_f64();
            // Compute throughput in bytes per second.
            let current_bytes_throughput = self.total_task_size / window_secs;
            // Compute normalized latency: seconds of latency per byte.
            let current_normalized_latency = if self.total_task_size > 0.0 {
                self.total_latency / self.total_task_size
            } else {
                0.0
            };

            // Decide whether to Increase or Decrease based on how the new metrics compare
            // with the baseline from the previous window.
            let action = if let (Some(prev_throughput), Some(prev_norm_latency)) = (
                self.pre_adjustment_bytes_throughput,
                self.pre_adjustment_normalized_latency,
            ) {
                // Calculate the ratio improvements.
                let throughput_ratio = if prev_throughput > 0.0 {
                    current_bytes_throughput / prev_throughput
                } else {
                    1.0
                };
                let latency_ratio = if prev_norm_latency > 0.0 {
                    current_normalized_latency / prev_norm_latency
                } else {
                    1.0
                };

                // Performance index: if throughput improves more than latency degrades, then Increase.
                if (throughput_ratio / latency_ratio) > 1.0 {
                    AdjustmentAction::Increase
                } else {
                    AdjustmentAction::Decrease
                }
            } else {
                // For the very first evaluation, default to Increase.
                AdjustmentAction::Increase
            };

            // Compute the new concurrency based on the chosen action.
            let new_concurrency = (match action {
                AdjustmentAction::Increase => {
                    ((self.max as f64) * RAMP_UP_MULTIPLIER).ceil() as usize
                }
                AdjustmentAction::Decrease => {
                    ((self.max as f64) * RAMP_DOWN_MULTIPLIER).floor() as usize
                }
            })
            .clamp(MIN_CONCURRENCY, MAX_CONCURRENCY);

            // Log adjustment details.
            self.obs.observe_state_ext(
                log::Level::Debug,
                "concurrency_adjustment",
                [
                    ("new_concurrency".into(), Value::U64(new_concurrency as u64)),
                    (
                        "current_bytes_throughput".into(),
                        Value::F64(current_bytes_throughput),
                    ),
                    (
                        "current_normalized_latency".into(),
                        Value::F64(current_normalized_latency),
                    ),
                    ("action".into(), Value::String(format!("{:?}", action))),
                ],
            );

            // Apply the adjustment.
            self.max = new_concurrency;
            // Save the current window’s metrics as the baseline for the next evaluation.
            self.pre_adjustment_bytes_throughput = Some(current_bytes_throughput);
            self.pre_adjustment_normalized_latency = Some(current_normalized_latency);
            // Reset the sliding window and all metrics.
            self.last_adjustment = now;
            self.completion_events.clear();
            self.total_latency = 0.0;
            self.latency_count = 0;
            self.total_task_size = 0.0;
        }
        // If no completions occur for a prolonged period, force a decrease.
        else if self.completion_events.is_empty() && elapsed_since_adjustment >= IDLE_TIMEOUT {
            let new_concurrency = self.max.saturating_sub(1).max(MIN_CONCURRENCY);
            self.obs.observe_state_ext(
                log::Level::Debug,
                "forced_decrease",
                [("new_concurrency".into(), Value::U64(new_concurrency as u64))],
            );
            self.max = new_concurrency;
            self.pre_adjustment_bytes_throughput = Some(0.0);
            self.pre_adjustment_normalized_latency = Some(0.0);
            self.last_adjustment = now;
        }
    }

    /// Return the current concurrency limit.
    pub fn get_concurrency(&self) -> usize {
        self.max
    }
}

//
// A helper type to wrap futures so that we can record their start times for latency measurement.
//

pin_project! {
    struct TrackedFuture<F> {
        #[pin]
        future: F,
        start_time: Instant,
    }
}

impl<F: Future> Future for TrackedFuture<F> {
    type Output = (F::Output, Duration);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.poll(cx) {
            Poll::Ready(output) => {
                let duration = Instant::now().duration_since(*this.start_time);
                Poll::Ready((output, duration))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

//
// The main stream combinator using the adaptive controller.
//

pin_project! {
    /// A stream combinator similar to [`buffer_unordered`] but with an in-band adaptive
    /// controller that tunes the concurrency limit based on sliding window metrics.
    #[must_use = "streams do nothing unless polled"]
    pub struct BufferAdaptiveUnordered<St>
    where
        St: Stream,
    {
        #[pin]
        stream: Fuse<St>,
        in_progress_queue: FuturesUnordered<TrackedFuture<St::Item>>,
        auto: AdaptiveConcurrency,
    }
}

impl<St> fmt::Debug for BufferAdaptiveUnordered<St>
where
    St: Stream + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferAutoUnordered")
            .field("stream", &self.stream)
            .field("in_progress_queue", &self.in_progress_queue)
            .field("concurrency", &self.auto.max)
            .finish()
    }
}

impl<St> BufferAdaptiveUnordered<St>
where
    St: Stream,
    St::Item: Future,
{
    /// Creates a new `BufferAutoUnordered` stream with the given initial concurrency limit.
    pub fn new(stream: St, initial_concurrency: usize) -> Self {
        Self {
            stream: stream.fuse(),
            in_progress_queue: FuturesUnordered::new(),
            auto: AdaptiveConcurrency::new(initial_concurrency),
        }
    }
}

impl<St> Stream for BufferAdaptiveUnordered<St>
where
    St: Stream,
    St::Item: Future,
    <St::Item as Future>::Output: TaskSize,
{
    type Item = <St::Item as Future>::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Update the adaptive controller.
        this.auto.tick();

        // Fill the in-progress queue until reaching the current concurrency limit.
        let current_max = this.auto.get_concurrency();
        while this.in_progress_queue.len() < current_max {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(fut)) => {
                    this.in_progress_queue.push(TrackedFuture {
                        future: fut,
                        start_time: Instant::now(),
                    });
                }
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        // Poll the in-progress futures.
        match this.in_progress_queue.poll_next_unpin(cx) {
            Poll::Ready(Some((output, duration))) => {
                // Record the completion event and its latency.
                this.auto.record_completion(duration, output.size());
                Poll::Ready(Some(output))
            }
            Poll::Ready(None) => {
                if this.stream.is_done() {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let queue_len = self.in_progress_queue.len();
        let (lower, upper) = self.stream.size_hint();
        (
            lower.saturating_add(queue_len),
            upper.and_then(|x| x.checked_add(queue_len)),
        )
    }
}

impl<St> FusedStream for BufferAdaptiveUnordered<St>
where
    St: Stream,
    St::Item: Future,
    <St::Item as Future>::Output: TaskSize,
{
    fn is_terminated(&self) -> bool {
        self.in_progress_queue.is_terminated() && self.stream.is_terminated()
    }
}

pub trait StreamAdaptive: Stream {
    /// Provides an extension method to use the adaptive, auto-tuning buffer.
    fn buffer_adaptive_unordered(self, n: usize) -> BufferAdaptiveUnordered<Self>
    where
        Self::Item: Future,
        <Self::Item as Future>::Output: TaskSize,
        Self: Sized,
    {
        BufferAdaptiveUnordered::new(self, n)
    }
}

impl<T: ?Sized> StreamAdaptive for T where T: Stream {}

pub trait TaskSize {
    fn size(&self) -> f64;
}

impl<T, E> TaskSize for Result<T, E>
where
    T: TaskSize,
{
    fn size(&self) -> f64 {
        match self {
            Ok(t) => t.size(),
            Err(_) => 1f64,
        }
    }
}
