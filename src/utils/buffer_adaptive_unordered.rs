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
use std::time::{Duration, Instant};

/// Specifies whether the adaptive controller should increase or decrease the concurrency limit.
#[derive(Debug, Copy, Clone)]
enum AdjustmentAction {
    Increase,
    Decrease,
}

/// Constants that control how the adaptive concurrency controller behaves.
///
/// - `RAMP_UP_THRESHOLD_MULTIPLIER` and `RAMP_DOWN_THRESHOLD_MULTIPLIER` determine the number
///   of completed tasks (scaled by the current concurrency limit) needed before considering an adjustment.
/// - `RAMP_UP_MULTIPLIER` and `RAMP_DOWN_MULTIPLIER` define the factor by which the concurrency is
///   scaled when increasing or decreasing respectively.
/// - `IDLE_TIMEOUT` is the duration after which, if no tasks have completed, a forced decrease is applied.
/// - `ADJUSTMENT_DEADTIME` prevents adjustments from happening too frequently.
const RAMP_UP_THRESHOLD_MULTIPLIER: usize = 10;
const RAMP_DOWN_THRESHOLD_MULTIPLIER: usize = 10;
const RAMP_UP_MULTIPLIER: f64 = std::f64::consts::SQRT_2;
const RAMP_DOWN_MULTIPLIER: f64 = std::f64::consts::FRAC_1_SQRT_2;
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const ADJUSTMENT_DEADTIME: Duration = Duration::from_secs(1);

/// AdaptiveConcurrency encapsulates the logic to adjust the concurrency limit dynamically based on task throughput.
///
/// It tracks:
/// - The current maximum concurrency (`max`).
/// - The number of completions since the last adjustment.
/// - The last measured throughput, which is used to compare against the current throughput.
/// - The last action taken (increase or decrease) to help decide future adjustments.
struct AdaptiveConcurrency {
    max: usize,
    last_adjustment: Instant,
    completions_since_adjustment: usize,
    last_throughput: Option<f64>,
    last_action: Option<AdjustmentAction>,
    obs: Observer<BaseObservable>,
}

impl AdaptiveConcurrency {
    /// Constructs a new adaptive concurrency controller with an initial concurrency limit.
    ///
    /// # Arguments
    ///
    /// * `initial` - The starting maximum number of concurrent tasks.
    pub fn new(initial: usize) -> Self {
        Self {
            max: initial,
            last_adjustment: Instant::now(),
            completions_since_adjustment: 0,
            last_throughput: None,
            last_action: None,
            obs: Observer::without_id("adaptive_concurrency"),
        }
    }

    /// Periodically checks if conditions are met to adjust the concurrency limit.
    ///
    /// The adjustment decision is based on:
    /// - The number of completed tasks since the last adjustment.
    /// - The elapsed time since the last adjustment.
    /// - Comparison of the current throughput (tasks per second) with the last measured throughput.
    ///
    /// If the throughput has improved, the last adjustment action is repeated (or defaults to increasing).
    /// Otherwise, the action is reversed. Additionally, if no completions have occurred for a specified timeout,
    /// a forced decrease is applied.
    pub fn tick(&mut self) {
        let current_concurrency = self.max;
        let threshold_multiplier = match self.last_action {
            Some(AdjustmentAction::Increase) => RAMP_UP_THRESHOLD_MULTIPLIER,
            Some(AdjustmentAction::Decrease) => RAMP_DOWN_THRESHOLD_MULTIPLIER,
            None => RAMP_UP_THRESHOLD_MULTIPLIER,
        };
        let threshold = current_concurrency * threshold_multiplier;
        let now = Instant::now();

        // Check if sufficient completions and time have passed to evaluate performance.
        if self.completions_since_adjustment >= threshold
            && now.duration_since(self.last_adjustment) >= ADJUSTMENT_DEADTIME
        {
            // Calculate current throughput: completions per second.
            let elapsed = now.duration_since(self.last_adjustment).as_secs_f64();
            let current_throughput = self.completions_since_adjustment as f64 / elapsed;
            // Determine whether to increase or decrease concurrency based on throughput change.
            let new_action = if let Some(last_tp) = self.last_throughput {
                if current_throughput > last_tp {
                    // Throughput increased: repeat previous action (or increase by default).
                    self.last_action.unwrap_or(AdjustmentAction::Increase)
                } else {
                    // Throughput decreased: reverse the previous action.
                    match self.last_action {
                        Some(AdjustmentAction::Increase) => AdjustmentAction::Decrease,
                        Some(AdjustmentAction::Decrease) => AdjustmentAction::Increase,
                        None => AdjustmentAction::Increase,
                    }
                }
            } else {
                AdjustmentAction::Increase
            };

            // Compute the new concurrency limit based on the decided action.
            let new_concurrency = match new_action {
                AdjustmentAction::Increase => usize::max(
                    ((current_concurrency as f64) * RAMP_UP_MULTIPLIER) as usize,
                    current_concurrency.saturating_add(1),
                ),
                AdjustmentAction::Decrease => usize::min(
                    ((current_concurrency as f64) * RAMP_DOWN_MULTIPLIER) as usize,
                    current_concurrency.saturating_sub(1),
                ),
            }
                .max(1);

            // Log the adjustment event with relevant metrics.
            self.obs.observe_state_ext(
                log::Level::Debug,
                "update".to_string(),
                [
                    (
                        "new_concurrency_limit".into(),
                        Value::U64(new_concurrency as u64),
                    ),
                    ("current_throughput".into(), Value::F64(current_throughput)),
                    (
                        "action".into(),
                        Value::String(match new_action {
                            AdjustmentAction::Increase => "increase".into(),
                            AdjustmentAction::Decrease => "decrease".into(),
                        }),
                    ),
                ],
            );

            // Update internal state based on the new adjustment.
            self.max = new_concurrency;
            self.last_throughput = Some(current_throughput);
            self.last_adjustment = now;
            self.completions_since_adjustment = 0;
            self.last_action = Some(new_action);
        }
        // If no tasks have completed for a defined idle period, enforce a decrease in concurrency.
        else if self.completions_since_adjustment == 0
            && now.duration_since(self.last_adjustment) >= IDLE_TIMEOUT
        {
            let new_concurrency = current_concurrency.saturating_sub(1).max(1);
            self.obs.observe_state_ext(
                log::Level::Debug,
                "forced decrease".to_string(),
                [(
                    "new_concurrency_limit".into(),
                    Value::U64(new_concurrency as u64),
                )],
            );
            self.max = new_concurrency;
            self.last_throughput = Some(0.0);
            self.last_adjustment = now;
            self.completions_since_adjustment = 0;
            self.last_action = Some(AdjustmentAction::Decrease);
        }
    }

    /// Returns the current maximum number of concurrently allowed tasks.
    pub fn get_concurrency(&self) -> usize {
        self.max
    }

    /// Increments the count of completed tasks.
    ///
    /// This should be called each time a task or future finishes execution.
    pub fn record_completion(&mut self) {
        self.completions_since_adjustment += 1;
    }
}

pin_project! {
    /// A stream combinator that concurrently processes items from an underlying stream with an adaptive
    /// concurrency limit.
    ///
    /// This combinator behaves similarly to [`buffer_unordered`] but automatically tunes the number of in-flight
    /// futures based on runtime throughput. It leverages an internal `AdaptiveConcurrency` controller to
    /// adjust the concurrency limit dynamically.
    ///
    /// The throughput is measured as the number of completed futures per second, and adjustments are made based on
    /// whether the throughput increases or decreases relative to previous measurements.
    #[must_use = "streams do nothing unless polled"]
    pub struct BufferAutoUnordered<St>
    where
        St: Stream,
    {
        #[pin]
        stream: Fuse<St>,
        in_progress_queue: FuturesUnordered<St::Item>,
        auto: AdaptiveConcurrency,
    }
}

impl<St> fmt::Debug for BufferAutoUnordered<St>
where
    St: Stream + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferUnordered")
            .field("stream", &self.stream)
            .field("in_progress_queue", &self.in_progress_queue)
            .field("concurrency", &self.auto.max)
            .field("last_adjustment", &self.auto.last_adjustment)
            .field(
                "completions_since_adjustment",
                &self.auto.completions_since_adjustment,
            )
            .field("last_throughput", &self.auto.last_throughput)
            .field("last_action", &self.auto.last_action)
            .finish()
    }
}

impl<St> BufferAutoUnordered<St>
where
    St: Stream,
    St::Item: Future,
{
    /// Creates a new `BufferAutoUnordered` stream with a given initial concurrency limit.
    ///
    /// # Arguments
    ///
    /// * `stream` - The underlying stream that produces futures.
    /// * `n` - The initial maximum number of futures to run concurrently.
    pub(super) fn new(stream: St, n: usize) -> Self {
        Self {
            stream: stream.fuse(),
            in_progress_queue: FuturesUnordered::new(),
            auto: AdaptiveConcurrency::new(n),
        }
    }
}

impl<St> Stream for BufferAutoUnordered<St>
where
    St: Stream,
    St::Item: Future,
{
    type Item = <St::Item as Future>::Output;

    /// Polls the stream, managing both the scheduling of new futures up to the current concurrency limit
    /// and the collection of completed futures.
    ///
    /// It performs the following steps:
    /// 1. Calls `tick` on the adaptive controller to possibly adjust the concurrency limit.
    /// 2. Polls the underlying stream to enqueue new futures until the current concurrency limit is reached.
    /// 3. Polls the in-progress futures and records their completions.
    /// 4. Returns the output of completed futures or indicates that the stream is pending.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Update the adaptive concurrency state.
        this.auto.tick();

        // Enqueue new futures until the concurrency limit is reached.
        let current_max = this.auto.get_concurrency();
        while this.in_progress_queue.len() < current_max {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(fut)) => {
                    this.in_progress_queue.push(fut);
                }
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        // Poll the futures in progress.
        match this.in_progress_queue.poll_next_unpin(cx) {
            Poll::Ready(Some(item)) => {
                // Mark the completed future and record the event.
                this.auto.record_completion();
                Poll::Ready(Some(item))
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

    /// Provides an estimation of the number of remaining items.
    ///
    /// The hint combines the size hint from the underlying stream and the number of in-progress futures.
    fn size_hint(&self) -> (usize, Option<usize>) {
        let queue_len = self.in_progress_queue.len();
        let (lower, upper) = self.stream.size_hint();
        (
            lower.saturating_add(queue_len),
            upper.and_then(|x| x.checked_add(queue_len)),
        )
    }
}

impl<St> FusedStream for BufferAutoUnordered<St>
where
    St: Stream,
    St::Item: Future,
{
    /// Returns `true` if both the underlying stream and the in-progress queue have terminated.
    fn is_terminated(&self) -> bool {
        self.in_progress_queue.is_terminated() && self.stream.is_terminated()
    }
}

/// An extension trait to provide a convenient method for wrapping a stream with adaptive buffering.
///
/// This trait allows any stream to be converted into a `BufferAutoUnordered` stream
/// by calling `.buffer_adaptive_unordered(n)`.
pub trait StreamAdaptive: Stream {
    fn buffer_adaptive_unordered(self, n: usize) -> BufferAutoUnordered<Self>
    where
        Self::Item: Future,
        Self: Sized,
    {
        BufferAutoUnordered::new(self, n)
    }
}

impl<T: ?Sized> StreamAdaptive for T where T: Stream {}
