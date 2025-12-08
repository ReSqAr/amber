use crate::db::error::DBError;
use crate::db::models::{LogOffset, Uid};
use crate::db::stores::kv::{Upsert, UpsertAction};
use crate::db::stores::log::{Offset, TailFrom, Writer};
use crate::db::stores::{kv, log};
use crate::flightdeck::tracked::mpsc_channel;
use crate::flightdeck::tracked::stream::Trackable;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryFutureExt};
use futures::{TryStreamExt, stream};
use futures_core::stream::BoxStream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
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

pub(crate) enum RowStatus<V> {
    Keep(V),
    Delete,
}

pub(crate) trait Status {
    type V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
    fn status(&self) -> RowStatus<Self::V>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Always();

impl Status for Always {
    type V = ();
    fn status(&self) -> RowStatus<Self::V> {
        RowStatus::Keep(())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct ValidFrom {
    pub(crate) valid_from: DateTime<Utc>,
}

#[derive(Clone)]
pub(crate) struct Store<K, V, S>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: Status + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    name: String,
    log: Writer<(Uid, K, V, S, ValidFrom)>,
    reduced: kv::Store<K, (V, S::V, ValidFrom)>,
    known_uids: kv::Store<Uid, ()>,
    reduced_offset: kv::Store<(), LogOffset>,
}

impl<K, V, S> Store<K, V, S>
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

        let name_cl = name.clone();
        let log =
            task::spawn_blocking(move || Writer::open(log_path, format!("reduced({name_cl})")))
                .map_err(DBError::from);
        let reduced = kv::Store::new(reduced_path, format!("reduced({name})"));
        let uids = kv::Store::new(uids_path, format!("known_uids({name})"));
        let offset = kv::Store::new(offset_path, format!("offset({name})"));

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

    pub(crate) async fn compact(&self) -> Result<(), DBError> {
        try_join!(
            self.log.compact().map_err(DBError::from),
            self.reduced.compact(),
            self.known_uids.compact(),
            self.reduced_offset.compact()
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

    #[allow(clippy::type_complexity)]
    pub(crate) fn current(&self) -> BoxStream<'static, Result<(K, V, S::V), DBError>> {
        self.reduced
            .stream()
            .map_ok(|(k, (v, vs, _))| (k, v, vs))
            .boxed()
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn left_join_current<IK, KF, E>(
        &self,
        s: BoxStream<'static, Result<IK, E>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<(V, S::V)>), E>>
    where
        KF: Fn(IK) -> K + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
        E: From<DBError> + Debug + Send + Sync + 'static,
    {
        self.reduced
            .left_join(s, key_func)
            .map_ok(|(k, o)| (k, o.map(|(v, vs, _)| (v, vs))))
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

    pub(crate) fn watermark(&self) -> Option<Offset> {
        self.log.watermark()
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

pub(crate) trait TransactionLike<V> {
    async fn put(&mut self, v: &V) -> Result<(), DBError>;
    async fn flush(&mut self) -> Result<(), DBError>;
    async fn close(self) -> Result<(), DBError>;
}

pub(crate) struct Transaction<K, V, S>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: Status + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    txn: log::Transaction<(Uid, K, V, S, ValidFrom)>,
    reducer: JoinHandle<Result<(), DBError>>,
}

impl<K, V, S> Transaction<K, V, S>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: Status + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn new(
        txn: log::Transaction<(Uid, K, V, S, ValidFrom)>,
        reduced: Store<K, V, S>,
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
        reduced: Store<K, V, S>,
        s: BoxStream<'static, Result<(Offset, (Uid, K, V, S, ValidFrom)), log::Error>>,
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
                move |e: Result<(Offset, (Uid, K, V, S, ValidFrom)), log::Error>| {
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
impl<K, V, S> TransactionLike<(Uid, K, V, S, ValidFrom)> for Transaction<K, V, S>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: Status + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn put(&mut self, v: &(Uid, K, V, S, ValidFrom)) -> Result<(), DBError> {
        task::block_in_place(|| self.txn.put_blocking(v))?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), DBError> {
        // TODO: spawn_blocking?
        task::block_in_place(|| self.txn.flush_blocking())?;
        Ok(())
    }

    async fn close(self) -> Result<(), DBError> {
        task::spawn_blocking(|| self.txn.close_blocking()).await??;
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
    type V = (V, S::V, ValidFrom);

    fn key(&self) -> Self::K {
        self.key.clone()
    }

    fn upsert(self, o: Option<Self::V>) -> UpsertAction<Self::V> {
        if let Some((_, _, valid_from)) = o
            && valid_from > self.valid_from
        {
            return UpsertAction::NoChange;
        }

        match self.status.status() {
            RowStatus::Keep(vs) => UpsertAction::Change((self.value, vs, self.valid_from)),
            RowStatus::Delete => UpsertAction::Delete,
        }
    }
}

#[allow(clippy::indexing_slicing)]
#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, TryStreamExt};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn new_uid(n: u64) -> Uid {
        n.into()
    }

    fn test_base_path(test_name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("reduced_store_tests_{}_{}", test_name, suffix));
        p
    }

    fn vf_at(ts: i64) -> ValidFrom {
        ValidFrom {
            valid_from: chrono::DateTime::from_timestamp(ts, 0).unwrap(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writes_show_up_in_current_view() -> Result<(), DBError> {
        let base = test_base_path("writes_show_up_in_current_view");

        // Use Always as the status type so everything is "kept".
        let store: Store<String, i32, Always> = Store::open("test_store".to_string(), base).await?;

        let mut txn = store.transaction().await?;

        let uid1 = new_uid(1);
        let uid2 = new_uid(2);

        let vf = vf_at(1_700_000_000);

        txn.put(&(uid1, "a".to_string(), 10, Always(), vf.clone()))
            .await?;
        txn.put(&(uid2, "b".to_string(), 20, Always(), vf.clone()))
            .await?;

        txn.flush().await?;
        txn.close().await?;

        let mut rows: Vec<_> = store.current().try_collect().await?;
        rows.sort_by(|(k1, ..), (k2, ..)| k1.cmp(k2));

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "a".to_string());
        assert_eq!(rows[0].1, 10);
        assert_eq!(rows[1].0, "b".to_string());
        assert_eq!(rows[1].1, 20);

        // After a transaction with writes, we should have some reduced offset.
        let offset = store.last_reduced_offset().await?;
        assert!(offset.is_some());

        store.close().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn older_valid_from_does_not_override_newer_value() -> Result<(), DBError> {
        let base = test_base_path("older_valid_from_does_not_override_newer_value");
        let store: Store<String, i32, Always> = Store::open("test_store".to_string(), base).await?;

        let mut txn = store.transaction().await?;

        let uid = new_uid(1);

        // First write: newer valid_from
        let newer_vf = vf_at(1_700_000_100);
        txn.put(&(uid, "k".to_string(), 100, Always(), newer_vf.clone()))
            .await?;

        // Second write: older valid_from, but appended *later* in the log.
        let older_vf = vf_at(1_700_000_000);
        txn.put(&(uid, "k".to_string(), 200, Always(), older_vf.clone()))
            .await?;

        txn.flush().await?;
        txn.close().await?;

        // We expect the value with the newer valid_from to win (100), because
        // UpsertValidFrom refuses to overwrite a newer row with an older one.
        let rows: Vec<_> = store.current().try_collect().await?;
        assert_eq!(rows.len(), 1);
        let (k, v, _status_view) = &rows[0];
        assert_eq!(k, "k");
        assert_eq!(*v, 100);

        store.close().await?;
        Ok(())
    }

    /// Custom status type to exercise the Delete branch of RowStatus.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct DeletingStatus {
        delete: bool,
    }

    impl Status for DeletingStatus {
        type V = ();

        fn status(&self) -> RowStatus<Self::V> {
            if self.delete {
                RowStatus::Delete
            } else {
                RowStatus::Keep(())
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delete_status_removes_row_from_current_view() -> Result<(), DBError> {
        let base = test_base_path("delete_status_removes_row_from_current_view");
        let store: Store<String, i32, DeletingStatus> =
            Store::open("test_store".to_string(), base).await?;

        let mut txn = store.transaction().await?;
        let uid = new_uid(1);

        let vf1 = vf_at(1_700_000_000);
        let vf2 = vf_at(1_700_000_100);

        // Insert a row with keep-status.
        txn.put(&(
            uid,
            "k".to_string(),
            123,
            DeletingStatus { delete: false },
            vf1,
        ))
        .await?;

        // Now insert a later row with delete-status; this should delete the key.
        txn.put(&(
            uid,
            "k".to_string(),
            999,
            DeletingStatus { delete: true },
            vf2,
        ))
        .await?;

        txn.flush().await?;
        txn.close().await?;

        let rows: Vec<_> = store.current().try_collect().await?;
        assert_eq!(rows.len(), 0, "Row should have been deleted");

        store.close().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn known_uids_are_marked_correctly() -> Result<(), DBError> {
        let base = test_base_path("known_uids_are_marked_correctly");
        let store: Store<String, i32, Always> = Store::open("test_store".to_string(), base).await?;

        let mut txn = store.transaction().await?;

        let uid_known = new_uid(1);
        let uid_also_known = new_uid(2);

        let vf = vf_at(1_700_000_000);

        txn.put(&(uid_known, "a".to_string(), 1, Always(), vf.clone()))
            .await?;
        txn.put(&(uid_also_known, "b".to_string(), 2, Always(), vf.clone()))
            .await?;

        txn.flush().await?;
        txn.close().await?;

        // Build a stream of Uids, one known and one unknown.
        let uid_unknown = new_uid(999);

        let input_stream: BoxStream<'static, Result<Uid, DBError>> =
            stream::iter(vec![Ok(uid_known), Ok(uid_unknown), Ok(uid_also_known)]).boxed();

        let results: Vec<_> = store
            .left_join_known_uids(input_stream, |u: Uid| u)
            .try_collect()
            .await?;

        // Turn into something easier to assert on.
        let mut seen_known = 0;
        let mut seen_unknown = 0;

        for (_, state) in results {
            match state {
                UidState::Known => seen_known += 1,
                UidState::Unknown => seen_unknown += 1,
            }
        }

        assert_eq!(seen_known, 2);
        assert_eq!(seen_unknown, 1);

        store.close().await?;
        Ok(())
    }
}
