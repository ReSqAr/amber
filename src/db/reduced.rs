use crate::db::error::DBError;
use crate::db::kvstore::{KVStore, Upsert, UpsertAction};
use crate::db::logstore;
use crate::db::logstore::{Offset, TailFrom, Writer};
use crate::db::models::{LogOffset, Uid};
use crate::flightdeck::tracked::mpsc_channel;
use crate::flightdeck::tracked::stream::Trackable;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryFutureExt};
use futures::{TryStreamExt, stream};
use futures_core::stream::BoxStream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::task::JoinHandle;
use tokio::{task, try_join};

const UID_BUFFER: usize = 100;

pub(crate) enum UidState {
    Known,
    Unknown,
}

pub(crate) enum RowStatus {
    Keep,
    Delete,
}

pub(crate) trait Status {
    fn status(&self) -> RowStatus;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct ValidFrom {
    pub(crate) valid_from: DateTime<Utc>,
}

#[derive(Clone)]
pub(crate) struct Reduced<K, V, S>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: Status + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    name: String,
    log: Writer<(Uid, K, V, S, ValidFrom)>,
    reduced: KVStore<K, (V, ValidFrom)>,
    known_uids: KVStore<Uid, ()>,
    reduced_offset: KVStore<(), LogOffset>,
}

impl<K, V, S> Reduced<K, V, S>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: Status + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    pub(crate) async fn open(name: String, base_path: PathBuf) -> Result<Self, DBError> {
        let log_path = base_path.join(format!("{name}.log"));
        let reduced_path = base_path.join("reduced.kv");
        let uids_path = base_path.join("known_uids.kv");
        let offset_path = base_path.join("offset.kv");

        let log = task::spawn_blocking(move || Writer::open(log_path)).map_err(DBError::from);
        let reduced = KVStore::new(reduced_path, format!("reduced({name})"));
        let uids = KVStore::new(uids_path, format!("known_uids({name})"));
        let offset = KVStore::new(offset_path, format!("offset({name})"));

        let (log, reduced, uids, offset) = try_join!(log, reduced, uids, offset)?;

        Ok(Self {
            name,
            log: log?,
            reduced,
            known_uids: uids,
            reduced_offset: offset,
        })
    }

    pub(crate) fn name(&self) -> &String {
        &self.name
    }

    pub(crate) async fn close(&self) -> Result<(), DBError> {
        try_join!(
            self.log.close().map_err(DBError::from),
            self.reduced.close(),
            self.known_uids.close(),
            self.reduced_offset.close()
        )?;
        Ok(())
    }

    pub(crate) async fn transaction(&self) -> Result<Transaction<K, V, S>, DBError> {
        let log = self.log.clone();
        let transaction = task::spawn_blocking(move || log.transaction()).await??;
        Transaction::new(transaction, self.clone()).await
    }

    pub(crate) async fn select(
        &self,
        from: Offset,
    ) -> BoxStream<'static, Result<(Offset, (Uid, K, V, S, ValidFrom)), DBError>> {
        self.log.reader().from(from).map_err(DBError::from).boxed()
    }

    pub(crate) fn current(&self) -> BoxStream<'static, Result<(K, V), DBError>> {
        self.reduced.stream().map_ok(|(k, (v, _))| (k, v)).boxed()
    }

    pub(crate) fn left_join_current<IK, KF>(
        &self,
        s: BoxStream<'static, Result<IK, DBError>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<V>), DBError>>
    where
        KF: Fn(IK) -> K + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.reduced
            .left_join(s, key_func)
            .map_ok(|(k, o)| (k, o.map(|(v, _)| v)))
            .boxed()
    }

    pub(crate) fn left_join_known_uids<IK, KF>(
        &self,
        s: BoxStream<'static, Result<IK, DBError>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, UidState), DBError>>
    where
        KF: Fn(IK) -> Uid + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.known_uids
            .left_join(s, key_func)
            .map_ok(|(key, state)| match state {
                Some(()) => (key, UidState::Known),
                None => (key, UidState::Unknown),
            })
            .boxed()
    }

    pub(crate) fn watermark(&self) -> Result<Option<Offset>, DBError> {
        Ok(self.log.watermark()?)
    }

    pub(crate) async fn last_reduced_offset(&self) -> Result<Option<Offset>, DBError> {
        let offset: Vec<_> = self
            .reduced_offset
            .left_join(stream::iter([Ok::<_, DBError>(())]).boxed(), |t| t)
            .collect()
            .await;

        match offset.into_iter().next() {
            Some(Ok(((), o))) => Ok(o.map(Offset::from)),
            Some(Err(e)) => Err(e),
            None => Err(DBError::InconsistencyError(
                "expected one offset".to_string(),
            )),
        }
    }
}

pub(crate) trait TransactionLike<K, V, S>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: Status + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn put(&mut self, v: &(Uid, K, V, S, ValidFrom)) -> Result<(), DBError>;
    async fn flush(&mut self) -> Result<(), DBError>;
    async fn close(self) -> Result<(), DBError>;
}

pub(crate) struct Transaction<K, V, S>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: Status + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    txn: logstore::Transaction<(Uid, K, V, S, ValidFrom)>,
    reducer: JoinHandle<Result<(), DBError>>,
}

impl<K, V, S> Transaction<K, V, S>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: Status + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn new(
        txn: logstore::Transaction<(Uid, K, V, S, ValidFrom)>,
        reduced: Reduced<K, V, S>,
    ) -> Result<Self, DBError> {
        let from = Self::next_offset(reduced.last_reduced_offset().await?);
        let tail = txn
            .tail(TailFrom::Offset(from))
            .track(format!("Transaction({})::log_stream", reduced.name));
        let reducer = Self::reduce(reduced, tail.boxed());

        Ok(Self { txn, reducer })
    }

    fn next_offset(i: Option<Offset>) -> Offset {
        match i {
            None => Offset::start(),
            Some(last_index) => last_index.increment(),
        }
    }

    #[allow(clippy::type_complexity)]
    fn reduce(
        reduced: Reduced<K, V, S>,
        s: BoxStream<'static, Result<(Offset, (Uid, K, V, S, ValidFrom)), logstore::Error>>,
    ) -> JoinHandle<Result<(), DBError>> {
        use tokio_stream::StreamExt as TokioStreamExt;

        tokio::spawn(async move {
            let (uid_tx, uid_rx) = mpsc_channel(format!("{}::uid", reduced.name), UID_BUFFER);
            let counter = Arc::new(AtomicUsize::new(0));
            let max_offset = Arc::new(AtomicU64::new(0));

            let counter_clone = counter.clone();
            let max_offset_clone = max_offset.clone();
            let s = TokioStreamExt::then(
                s,
                move |e: Result<(Offset, (Uid, K, V, S, ValidFrom)), logstore::Error>| {
                    let uid_tx = uid_tx.clone();
                    let counter_clone = counter_clone.clone();
                    let max_offset_clone = max_offset_clone.clone();
                    async move {
                        match e {
                            Ok((o, (uid, k, v, s, vf))) => {
                                counter_clone.fetch_add(1, Ordering::Relaxed);
                                max_offset_clone.store(o.into(), Ordering::SeqCst);
                                uid_tx.send(Ok((uid, Some(())))).await?;
                                Ok(UpsertValidFrom {
                                    key: k,
                                    value: v,
                                    status: s,
                                    valid_from: vf,
                                })
                            }
                            Err(e) => Err::<_, DBError>(e.into()),
                        }
                    }
                },
            )
            .boxed();

            try_join!(
                reduced.reduced.upsert(s),
                reduced.known_uids.apply(uid_rx.boxed()),
            )?;

            let counter = counter.load(Ordering::Relaxed);
            if counter > 0 {
                let max_offset = max_offset.load(Ordering::SeqCst);
                reduced
                    .reduced_offset
                    .apply(
                        stream::iter([Ok::<_, DBError>(((), Some(LogOffset(max_offset))))]).boxed(),
                    )
                    .await?;
            }

            Ok(())
        })
    }
}
impl<K, V, S> TransactionLike<K, V, S> for Transaction<K, V, S>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: Status + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn put(&mut self, v: &(Uid, K, V, S, ValidFrom)) -> Result<(), DBError> {
        task::block_in_place(|| self.txn.put(v))?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), DBError> {
        // TODO: spawn_blocking?
        task::block_in_place(|| self.txn.flush())?;
        Ok(())
    }

    async fn close(self) -> Result<(), DBError> {
        task::spawn_blocking(|| self.txn.close()).await??;
        self.reducer.await??;
        Ok(())
    }
}

pub(crate) struct UpsertValidFrom<K, V, S>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: Status + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    key: K,
    value: V,
    status: S,
    valid_from: ValidFrom,
}

impl<K, V, S> Upsert for UpsertValidFrom<K, V, S>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: Status + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    type K = K;
    type V = (V, ValidFrom);

    fn key(&self) -> Self::K {
        self.key.clone()
    }

    fn upsert(self, o: Option<Self::V>) -> UpsertAction<Self::V> {
        if let Some((_, valid_from)) = o
            && valid_from > self.valid_from
        {
            return UpsertAction::NoChange;
        }

        match self.status.status() {
            RowStatus::Keep => UpsertAction::Change((self.value, self.valid_from)),
            RowStatus::Delete => UpsertAction::Delete,
        }
    }
}
