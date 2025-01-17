use futures::Stream;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

/// A stream adapter that yields only the 'ok' items (I) from an upstream
/// `Stream<Item = Result<I, E>>`, stopping after the first encountered error.
struct ErrorTrackingStream<S, E> {
    upstream: S,
    shared_error: Arc<Mutex<Option<E>>>,
}

impl<S, I, E> Stream for ErrorTrackingStream<S, E>
where
    S: Stream<Item = Result<I, E>> + Unpin,
{
    type Item = I;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // If we've already recorded an error, produce no more items.
        if self.shared_error.lock().unwrap().is_some() {
            return Poll::Ready(None);
        }

        match Pin::new(&mut self.upstream).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(item)),
            Poll::Ready(Some(Err(e))) => {
                *self.shared_error.lock().unwrap() = Some(e);
                Poll::Ready(None)
            }
        }
    }
}

/// Extension trait that adds `try_forward_into` to any `Stream<Item = Result<I, E>>`.
pub trait TryForwardIntoExt<'a, I, E>: Stream<Item = Result<I, E>> + Sized {
    /// Pipes all `Ok(I)` items into a function `f` that consumes a plain `Stream<Item = I>`,
    /// stopping at the first error from this stream. Afterwards:
    /// - If the stream encountered an error first, return it;
    /// - Otherwise, return the result of `f` (which could be success or error).
    async fn try_forward_into<Fut, O, F>(self, f: F) -> Result<O, E>
    where
        // `f` is a function taking a `Stream<Item = I>` and producing a `Future<Output = Result<O, E>>`
        F: FnOnce(ErrorTrackingStream<Self, E>) -> Fut + Send + 'a,
        Fut: Future<Output = Result<O, E>> + Send + 'a,
        I: Send + 'a,
        E: Send + 'a,
        Self: Unpin + Send + 'a,
        O: Send + 'a;
}

impl<'a, S, I, E> TryForwardIntoExt<'a, I, E> for S
where
    S: Stream<Item = Result<I, E>> + Unpin + Send + 'a,
    I: Send + 'a,
    E: Send + 'a,
{
    async fn try_forward_into<Fut, O, F>(self, f: F) -> Result<O, E>
    where
        F: FnOnce(ErrorTrackingStream<Self, E>) -> Fut + Send + 'a,
        Fut: Future<Output = Result<O, E>> + Send + 'a,
        O: Send + 'a,
    {
        let shared_error = Arc::new(Mutex::new(None));
        let adapter = ErrorTrackingStream {
            upstream: self,
            shared_error: shared_error.clone(),
        };

        // Run `f`, which sees a clean stream of I, with no embedded errors
        let result = f(adapter).await;

        // If the upstream encountered an error, return that first
        if let Some(err) = shared_error.lock().unwrap().take() {
            return Err(err);
        }

        // Otherwise return what `f` produced (which may be an error or success)
        result
    }
}
