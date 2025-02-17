use futures::Stream;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

/// A stream adapter that yields only the 'Ok' items (I) from an upstream
/// `Stream<Item = Result<I, EStream>>`, stopping after the first encountered error.
pub(crate) struct ErrorTrackingStream<S, I, EStream> {
    upstream: S,
    shared_error: Arc<Mutex<Option<EStream>>>,
    _marker: std::marker::PhantomData<I>,
}

impl<S, I, EStream> ErrorTrackingStream<S, I, EStream> {
    pub(crate) fn new(upstream: S, shared_error: Arc<Mutex<Option<EStream>>>) -> Self {
        Self {
            upstream,
            shared_error,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, I, EStream> Unpin for ErrorTrackingStream<S, I, EStream> where S: Unpin {}

impl<S, I, EStream> Stream for ErrorTrackingStream<S, I, EStream>
where
    S: Stream<Item = Result<I, EStream>> + Unpin,
{
    type Item = I;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // If we've already recorded an error, produce no more items.
        if self.shared_error.lock().unwrap().is_some() {
            return Poll::Ready(None);
        }

        // Safely get a mutable reference to `self`.
        let this = self.get_mut();

        match Pin::new(&mut this.upstream).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(item)),
            Poll::Ready(Some(Err(e))) => {
                *this.shared_error.lock().unwrap() = Some(e);
                Poll::Ready(None)
            }
        }
    }
}

/// Extension trait that adds `try_forward_into` to any `Stream<Item = Result<I, EStream>>`.
pub(crate) trait TryForwardIntoExt<I, EStream>:
    Stream<Item = Result<I, EStream>> + Sized
{
    /// Pipes all `Ok(I)` items into a function `f` that consumes a plain `Stream<Item = I>`,
    /// stopping at the first error from this stream. Afterwards:
    /// - If the stream encountered an error first, return it converted into `EFinal`;
    /// - Otherwise, return the result of `f` converted into `EFinal`.
    async fn try_forward_into<Fut, O, F, EConsumer, EFinal>(self, f: F) -> Result<O, EFinal>
    where
        // `f` is a function taking an `ErrorTrackingStream<Self, I, EStream>` and producing a `Future<Output = Result<O, EConsumer>>`
        F: FnOnce(ErrorTrackingStream<Self, I, EStream>) -> Fut + Send,
        Fut: Future<Output = Result<O, EConsumer>> + Send,
        EStream: Into<EFinal>,
        EConsumer: Into<EFinal>,
        I: Send,
        Self: Unpin + Send,
        O: Send,
        EFinal: Send,
    {
        let shared_error = Arc::new(Mutex::new(None));
        let adapter = ErrorTrackingStream::new(self, shared_error.clone());

        let result = f(adapter).await;

        // If the upstream encountered an error, return that first converted into EFinal
        if let Some(err) = shared_error.lock().unwrap().take() {
            return Err(err.into());
        }

        // Otherwise, convert the consumer's result into EFinal
        result.map_err(|e| e.into())
    }
}

impl<S, I, EStream> TryForwardIntoExt<I, EStream> for S where
    S: Stream<Item = Result<I, EStream>> + Unpin + Send
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_try_forward_into_ext() {
        let items = vec![
            Ok::<i32, Box<dyn std::error::Error + Send + Sync>>(1),
            Ok(2),
            Ok(3),
        ];
        let my_stream = futures::stream::iter(items).boxed();

        let result = my_stream
            .try_forward_into::<_, _, _, _, Box<dyn std::error::Error + Send + Sync>>(
                |s| async move {
                    let mut sum = 0;
                    futures::pin_mut!(s);
                    while let Some(x) = s.next().await {
                        sum += x;
                    }
                    Ok::<i32, Box<dyn std::error::Error + Send + Sync>>(sum)
                },
            )
            .await;
        assert_eq!(result.unwrap(), 6);
    }
}
