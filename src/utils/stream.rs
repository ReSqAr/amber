use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use pin_project_lite::pin_project;
use tokio::time::{self, Sleep};

pub trait BoundedWaitChunksExt: Stream + Sized {
    fn bounded_wait_chunks(self, max_items: usize, max_wait: Duration) -> BoundedWaitChunks<Self> {
        assert!(max_items > 0, "max_items must be > 0");
        BoundedWaitChunks {
            inner: self,
            max_items,
            max_wait,
            buf: Vec::with_capacity(max_items),
            deadline: None,
            inner_done: false,
        }
    }
}

impl<S: Stream> BoundedWaitChunksExt for S {}

pin_project! {
    pub struct BoundedWaitChunks<S: Stream> {
        #[pin]
        inner: S,
        max_items: usize,
        max_wait: Duration,
        buf: Vec<S::Item>,
        #[pin]
        deadline: Option<Sleep>,
        inner_done: bool,
    }
}

impl<S> Stream for BoundedWaitChunks<S>
where
    S: Stream,
{
    type Item = Vec<S::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // If inner already ended, flush remaining buffer or end.
        if *this.inner_done {
            if this.buf.is_empty() {
                return Poll::Ready(None);
            } else {
                let out = std::mem::take(this.buf);
                return Poll::Ready(Some(out));
            }
        }

        // If we have a running timer, see if it fired; if yes, flush the batch.
        #[allow(clippy::collapsible_if)]
        if let Some(mut d) = this.deadline.as_mut().as_pin_mut() {
            if Pin::new(&mut d).poll(cx).is_ready() && !this.buf.is_empty() {
                // Timer elapsed: flush what we have.
                this.deadline.set(None);
                let out = std::mem::take(this.buf);
                return Poll::Ready(Some(out));
            }
        }

        let mut do_wake = false;
        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if this.buf.is_empty() {
                        // First item: start the per-batch timer.
                        this.deadline.set(Some(time::sleep(*this.max_wait)));
                        do_wake = true;
                    }

                    this.buf.push(item);
                    if this.buf.len() >= *this.max_items {
                        // Cap reached: flush.
                        this.deadline.set(None);
                        let out = std::mem::take(this.buf);
                        return Poll::Ready(Some(out));
                    }

                    // Continue the loop to grab more immediately-ready items.
                    continue;
                }
                Poll::Ready(None) => {
                    *this.inner_done = true;
                    if this.buf.is_empty() {
                        // Nothing buffered: end of stream.
                        return Poll::Ready(None);
                    } else {
                        // Flush the final partial batch.
                        this.deadline.set(None);
                        let out = std::mem::take(this.buf);
                        return Poll::Ready(Some(out));
                    }
                }
                Poll::Pending => {
                    if this.buf.is_empty() {
                        // No batch in progress; yield Pending.
                        return Poll::Pending;
                    } else {
                        if do_wake {
                            cx.waker().wake_by_ref();
                        }

                        // Batch started: keep the task alive until either:
                        // - more items become ready, or
                        // - the deadline fires (we already poll it above).
                        // We return Pending here so the waker from either source wakes us.
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}
