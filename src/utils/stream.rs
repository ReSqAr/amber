use async_stream::stream;
use futures_core::Stream;
use tokio_stream::StreamExt;

pub fn group_by_key<S, K, F>(s: S, key_fn: F) -> impl Stream<Item = (K, Vec<S::Item>)>
where
    S: Stream + Unpin,
    F: Fn(&S::Item) -> K + Clone,
    K: PartialEq + Clone,
{
    let mut s = s;
    stream! {
        use std::collections::VecDeque;
        let mut current_key = None;
        let mut buffer = VecDeque::new();

        while let Some(item) = s.next().await {
            let k = key_fn(&item);
            if current_key.as_ref() == Some(&k) {
                buffer.push_back(item);
            } else {
                if let Some(prev_key) = current_key.replace(k.clone()) {
                    let items: Vec<_> = buffer.drain(..).collect();
                    yield (prev_key, items);
                }
                buffer.push_back(item);
            }
        }
        if let Some(k) = current_key {
            let items: Vec<_> = buffer.into_iter().collect();
            yield (k, items);
        }
    }
}
