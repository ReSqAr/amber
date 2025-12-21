use crate::db::error::DBError;
use crate::db::stores::guard::DatabaseGuard;
use crate::db::stores::options;
use crate::flightdeck::tracer::Tracer;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use rocksdb::{DB, Direction, IteratorMode, WriteOptions};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicU64, Ordering},
};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;

type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Offset(pub(crate) u64);

impl Offset {
    pub fn start() -> Self {
        Offset(0)
    }

    pub fn increment(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    pub fn saturating_add(self, o: u32) -> Self {
        Self(self.0.saturating_add(o as u64))
    }
}

impl From<u64> for Offset {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Offset> for u64 {
    fn from(o: Offset) -> Self {
        o.0
    }
}

#[derive(Clone, Copy, Debug)]
pub enum TailFrom {
    #[allow(dead_code)]
    Head,
    #[allow(dead_code)]
    Offset(Offset),
    #[allow(dead_code)]
    Start,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("an active transaction is already in progress")]
    ActiveTransactionInProgress,

    #[error("transaction is closed")]
    ClosedTransaction,

    #[error("lock was poisoned")]
    Poisoned,

    #[error("rocksdb error: {0}")]
    Rocksdb(#[from] rocksdb::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] postcard::Error),

    #[error("failed to convert key")]
    KeyConversion,

    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),

    #[error("{0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("database accessed after close")]
    AccessAfterDrop,
}

const WATERMARK_SENTINEL: u64 = u64::MAX;

struct Inner {
    name: String,
    db: Arc<parking_lot::RwLock<DatabaseGuard>>,
    // Next offset - committed or uncommitted
    next_offset: AtomicU64,
    // Last durably committed offset (None = empty).
    watermark: AtomicU64,
    watermark_tx: watch::Sender<Option<Offset>>,
    active_tx: RwLock<bool>,
}

impl Inner {
    fn current_watermark(&self) -> Option<Offset> {
        let w = self.watermark.load(Ordering::SeqCst);
        if w == WATERMARK_SENTINEL {
            None
        } else {
            Some(w.into())
        }
    }
}

#[derive(Clone)]
pub struct Writer<V>
where
    V: Serialize + DeserializeOwned + Send + 'static,
{
    inner: Arc<Inner>,
    _marker: PhantomData<V>,
}

#[derive(Clone)]
pub struct Reader<V>
where
    V: DeserializeOwned + Send + 'static,
{
    inner: Arc<Inner>,
    _marker: PhantomData<V>,
}

pub struct Transaction<V>
where
    V: Serialize + DeserializeOwned + Send + 'static,
{
    inner: Arc<Inner>,
    write_opt: WriteOptions,
    /// Last offset written by this transaction (if any).
    last_offset: Option<Offset>,
    tail_cancels: Mutex<Vec<tokio::sync::oneshot::Sender<()>>>,
    closed: bool,
    _marker: PhantomData<V>,
}

fn offset_to_key(offset: u64) -> [u8; 8] {
    offset.to_be_bytes()
}

fn offset_from_key(key: &[u8]) -> Result<Offset> {
    if key.len() != 8 {
        return Err(Error::KeyConversion);
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(key);
    Ok(Offset(u64::from_be_bytes(buf)))
}

fn max_offset_from_db(db: &DB) -> Result<Option<Offset>> {
    let mut iter = db.iterator(IteratorMode::End);
    iter.next()
        .transpose()?
        .map(|(k, _)| offset_from_key(&k))
        .transpose()
}

impl<V> Writer<V>
where
    V: Serialize + DeserializeOwned + Send + 'static,
{
    pub fn open(path: PathBuf, name: String) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let opts = options::make_options();
        let tracer = Tracer::new_on(format!("log::Writer({})::open", name));
        let db = DB::open(&opts, &path)?;
        let max_offset = max_offset_from_db(&db)?;
        tracer.measure();

        let next_offset = AtomicU64::new(match max_offset {
            Some(o) => o.0.saturating_add(1),
            None => 0,
        });
        let watermark = AtomicU64::new(match max_offset {
            Some(o) => o.0,
            None => WATERMARK_SENTINEL,
        });
        let (watermark_tx, _) = watch::channel(max_offset);

        let guard_name = name.clone();
        let db = Arc::new(parking_lot::RwLock::new(DatabaseGuard {
            db: Some(db),
            name: guard_name,
        }));
        let inner = Inner {
            name,
            db,
            next_offset,
            watermark,
            watermark_tx,
            active_tx: RwLock::new(false),
        };

        Ok(Self {
            inner: Arc::new(inner),
            _marker: PhantomData,
        })
    }

    pub async fn close(&self) -> Result<()> {
        let db = self.inner.db.clone();
        task::spawn_blocking(move || {
            let mut guard = db.write();
            guard.close_sync();
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn compact(&self) -> std::result::Result<(), DBError> {
        let tracer = Tracer::new_on(format!("Writer({})::compact", self.inner.name));
        let db = self.inner.db.clone();
        task::spawn_blocking(move || {
            let mut guard = db.write();
            let db_ref = guard.as_mut().ok_or(DBError::AccessAfterDrop)?;
            db_ref.compact_range(None::<&[u8]>, None::<&[u8]>);
            Ok::<_, DBError>(())
        })
        .await??;
        tracer.measure();
        Ok(())
    }

    pub fn watermark(&self) -> Option<Offset> {
        self.inner.current_watermark()
    }

    pub fn reader(&self) -> Reader<V> {
        Reader {
            inner: Arc::clone(&self.inner),
            _marker: PhantomData,
        }
    }

    pub fn transaction(&self) -> Result<Transaction<V>> {
        let mut guard = self.inner.active_tx.write().map_err(|_| Error::Poisoned)?;

        if *guard {
            return Err(Error::ActiveTransactionInProgress);
        }
        *guard = true;

        Ok(Transaction {
            inner: Arc::clone(&self.inner),
            write_opt: options::write_options(),
            last_offset: None,
            tail_cancels: Mutex::new(Vec::new()),
            closed: false,
            _marker: PhantomData,
        })
    }
}

fn ser<T: Serialize + ?Sized>(v: &T) -> std::result::Result<Vec<u8>, Error> {
    Ok(postcard::to_stdvec(v)?)
}

fn de<T: DeserializeOwned>(bytes: &[u8]) -> std::result::Result<T, Error> {
    Ok(postcard::from_bytes(bytes)?)
}

impl<V> Transaction<V>
where
    V: Serialize + DeserializeOwned + Send + 'static,
{
    pub fn put_blocking(&mut self, v: &V) -> Result<()> {
        if self.closed {
            return Err(Error::ClosedTransaction);
        }

        let offset_u64 = self.inner.next_offset.fetch_add(1, Ordering::SeqCst);
        let offset = Offset(offset_u64);

        let key = offset_to_key(offset_u64);
        let bytes = ser(v)?;

        let guard = self.inner.db.read();
        let db_ref = guard.as_ref().ok_or(Error::AccessAfterDrop)?;
        db_ref.put_opt(key, &bytes, &self.write_opt)?;
        self.last_offset = Some(offset);

        Ok(())
    }

    pub fn flush_blocking(&mut self) -> Result<Option<Offset>> {
        self.persist_and_publish_blocking()
    }

    pub fn close_blocking(mut self) -> Result<Option<Offset>> {
        let res = self.persist_and_publish_blocking();
        self.closed = true;

        if let Ok(mut guard) = self.tail_cancels.lock() {
            for sender in guard.drain(..) {
                let _ = sender.send(());
            }
        }

        if let Ok(mut g) = self.inner.active_tx.write() {
            *g = false;
        }

        res
    }

    fn persist_and_publish_blocking(&mut self) -> Result<Option<Offset>> {
        let Some(offset) = self.last_offset else {
            return Ok(None);
        };

        let tracer = Tracer::new_on(format!(
            "log::Transaction({})::persist_and_publish_blocking",
            self.inner.name
        ));
        let guard = self.inner.db.read();
        let db_ref = guard.as_ref().ok_or(Error::AccessAfterDrop)?;
        db_ref.flush()?;
        tracer.measure();

        self.inner.watermark.store(offset.0, Ordering::SeqCst);

        let _ = self.inner.watermark_tx.send(Some(offset));

        Ok(Some(offset))
    }

    pub fn tail(&self, from: impl Into<TailFrom>) -> BoxStream<'static, Result<(Offset, V)>> {
        let db = Arc::clone(&self.inner.db);
        let mut watermark_rx = self.inner.watermark_tx.subscribe();
        let (tx, rx) = mpsc::channel::<Result<(Offset, V)>>(128);

        let watermark_snapshot = self.inner.current_watermark();

        let (start_offset, include_existing) = match from.into() {
            TailFrom::Start => (Offset::start(), true),
            TailFrom::Offset(o) => (o, true),
            TailFrom::Head => {
                let start = watermark_snapshot
                    .map(Offset::increment)
                    .unwrap_or_else(Offset::start);
                (start, false)
            }
        };

        // Per-tail cancellation handle stored in the Transaction as before
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        if let Ok(mut guard) = self.tail_cancels.lock() {
            guard.push(cancel_tx);
        }

        tokio::spawn(async move {
            let mut next = start_offset;

            // 1. Initial snapshot up to current watermark (if requested).
            if include_existing
                && let Some(wm) = watermark_snapshot
                && wm.0 >= next.0
            {
                if let Err(join_err) = tokio::task::spawn_blocking({
                    let db = db.clone();
                    let tx = tx.clone();
                    move || pump_range_blocking::<V>(db, next, wm, tx)
                })
                .await
                {
                    eprintln!("tail initial snapshot join error: {join_err:?}");
                    return;
                }

                next = wm.increment();
            }

            // 2. Live tail: watermark updates vs cancellation.
            loop {
                tokio::select! {
                    // Cancellation requested by Transaction::close().
                    _ = &mut cancel_rx => {
                        // Final drain up to *current* watermark, then exit.
                        let new_wm = *watermark_rx.borrow();
                        if let Some(wm) = new_wm && wm.0 >= next.0 &&
                            let Err(join_err) = tokio::task::spawn_blocking({
                                    let db = db.clone();
                                    let tx = tx.clone();
                                    move || pump_range_blocking::<V>(db, next, wm, tx)
                               }).await {
                                eprintln!("tail final drain join error: {join_err:?}");
                            }
                        break;
                    }

                    // Normal watermark updates while transaction is still open.
                    changed = watermark_rx.changed() => {
                        if changed.is_err() {
                            // Sender dropped (DB/Writer shutdown).
                            break;
                        }

                        let new_wm = *watermark_rx.borrow();
                        let Some(wm) = new_wm else { continue };

                        if wm.0 < next.0 {
                            continue;
                        }

                        if let Err(join_err) = tokio::task::spawn_blocking({
                            let db = db.clone();
                            let tx = tx.clone();
                            move || pump_range_blocking::<V>(db, next, wm, tx)
                        }).await {
                            eprintln!("tail live join error: {join_err:?}");
                            break;
                        }

                        next = wm.increment();
                    }
                }
            }
            // Dropping tx here will eventually close the ReceiverStream once all senders are gone.
        });

        ReceiverStream::new(rx).boxed()
    }
}

impl<V> Reader<V>
where
    V: DeserializeOwned + Send + 'static,
{
    #[allow(dead_code)]
    pub fn watermark(&self) -> Option<Offset> {
        self.inner.current_watermark()
    }

    pub fn from(&self, from: Offset) -> BoxStream<'static, Result<(Offset, V)>> {
        let upper = match self.inner.current_watermark() {
            Some(w) if from.0 <= w.0 => w,
            Some(_) | None => return stream::empty().boxed(),
        };

        let db = Arc::clone(&self.inner.db);
        let (tx, rx) = mpsc::channel::<Result<(Offset, V)>>(128);

        tokio::task::spawn_blocking(move || {
            pump_range_blocking::<V>(db, from, upper, tx);
        });

        ReceiverStream::new(rx).boxed()
    }
}

fn pump_range_blocking<V>(
    db: Arc<parking_lot::RwLock<DatabaseGuard>>,
    from: Offset,
    to: Offset,
    tx: mpsc::Sender<Result<(Offset, V)>>,
) where
    V: DeserializeOwned + Send + 'static,
{
    let guard = db.read();
    let db = match guard.as_ref().ok_or(Error::AccessAfterDrop) {
        Ok(db) => db,
        Err(e) => {
            let _ = tx.blocking_send(Err(e));
            return;
        }
    };

    let start_key = offset_to_key(from.0);
    let iter = db.iterator(IteratorMode::From(&start_key, Direction::Forward));

    for row in iter {
        let (k, v) = match row {
            Ok((k, v)) => (k, v),
            Err(e) => {
                let _ = tx.blocking_send(Err(Error::Rocksdb(e)));
                break;
            }
        };

        let off = match offset_from_key(&k) {
            Ok(off) => off,
            Err(e) => {
                let _ = tx.blocking_send(Err(e));
                break;
            }
        };

        if off.0 < from.0 {
            continue;
        }
        if off.0 > to.0 {
            break;
        }

        let value = match de::<V>(&v) {
            Ok(v) => v,
            Err(e) => {
                let _ = tx.blocking_send(Err(e));
                break;
            }
        };

        if tx.blocking_send(Ok((off, value))).is_err() {
            // Receiver dropped; nothing more to do.
            break;
        }
    }
    // tx dropped by caller when stream ends.
}

#[allow(clippy::indexing_slicing)]
#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use tempfile::TempDir;

    fn make_writer<V>() -> Writer<V>
    where
        V: Serialize + DeserializeOwned + Send + 'static,
    {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().to_path_buf();

        let (watermark_tx, _watermark_rx) = watch::channel(None);

        let opts = options::make_options();
        let db = DB::open(&opts, path).expect("open db");
        let db = Arc::new(parking_lot::RwLock::new(DatabaseGuard {
            db: Some(db),
            name: "test".to_string(),
        }));

        let inner = Arc::new(Inner {
            name: "test".to_string(),
            db,
            next_offset: AtomicU64::new(0),
            watermark: AtomicU64::new(WATERMARK_SENTINEL),
            watermark_tx,
            active_tx: RwLock::new(false),
        });

        Writer {
            inner,
            _marker: PhantomData,
        }
    }

    #[tokio::test]
    async fn write_and_read_back_strings() {
        let writer: Writer<String> = make_writer();

        // Write a few items.
        let mut tx = writer.transaction().expect("start transaction");
        tx.put_blocking(&"one".to_string()).expect("put one");
        tx.put_blocking(&"two".to_string()).expect("put two");
        tx.put_blocking(&"three".to_string()).expect("put three");

        // Close (fsync + publish watermark).
        let last = tx.close_blocking().expect("close transaction");
        assert_eq!(last, Some(Offset(2)));

        // Read back from the start.
        let reader = writer.reader();
        let stream = reader.from(Offset::start());

        let items: Vec<(Offset, String)> = stream.try_collect().await.expect("collect stream");

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, Offset(0));
        assert_eq!(items[0].1, "one");
        assert_eq!(items[1].0, Offset(1));
        assert_eq!(items[1].1, "two");
        assert_eq!(items[2].0, Offset(2));
        assert_eq!(items[2].1, "three");

        writer.close().await.expect("close writer");
    }

    #[tokio::test]
    async fn from_respects_watermark() {
        let writer: Writer<u64> = make_writer();

        // Write some items, but only commit part of them.
        let mut tx = writer.transaction().expect("start transaction");
        tx.put_blocking(&10).expect("put 10");
        tx.put_blocking(&20).expect("put 20");

        // First flush: watermark at offset 1
        let last = tx.flush_blocking().expect("flush tx");
        assert_eq!(last, Some(Offset(1)));

        // Add one more unflushed write
        tx.put_blocking(&30).expect("put 30 (unflushed)");

        // Now read from offset 0; we should only see the first two.
        let reader = writer.reader();
        let stream = reader.from(Offset::start());
        let items: Vec<(Offset, u64)> = stream.try_collect().await.expect("collect stream");

        assert_eq!(items.len(), 2);
        assert_eq!(items[0], (Offset(0), 10));
        assert_eq!(items[1], (Offset(1), 20));

        writer.close().await.expect("close writer");
    }

    #[tokio::test]
    async fn writer_new_recovers_watermark_and_offsets() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("db");

        // First instance: write some data and close.
        {
            let writer: Writer<String> =
                Writer::open(path.clone(), "test".to_string()).expect("create writer");

            let mut tx = writer.transaction().expect("start tx");
            tx.put_blocking(&"alpha".to_string()).expect("put alpha");
            tx.put_blocking(&"beta".to_string()).expect("put beta");
            tx.put_blocking(&"gamma".to_string()).expect("put gamma");

            let last = tx.close_blocking().expect("close tx");
            assert_eq!(last, Some(Offset(2)));

            let wm = writer.watermark();
            assert_eq!(wm, Some(Offset(2)));

            writer.close().await.expect("close writer");
        }

        // Second instance: reopen and verify recovery.
        {
            let writer: Writer<String> =
                Writer::open(path.clone(), "test".to_string()).expect("reopen writer");

            // Watermark should be recovered.
            let wm = writer.watermark();
            assert_eq!(wm, Some(Offset(2)));

            // Reading from the start should give us the same data.
            let reader = writer.reader();
            let stream = reader.from(Offset::start());
            let items: Vec<(Offset, String)> =
                stream.try_collect().await.expect("collect after reopen");

            assert_eq!(items.len(), 3);
            assert_eq!(items[0], (Offset(0), "alpha".to_string()));
            assert_eq!(items[1], (Offset(1), "beta".to_string()));
            assert_eq!(items[2], (Offset(2), "gamma".to_string()));

            // Next transaction should continue from offset 3.
            let mut tx = writer.transaction().expect("start tx 2");
            tx.put_blocking(&"delta".to_string()).expect("put delta");
            let last = tx.close_blocking().expect("close tx 2");
            assert_eq!(last, Some(Offset(3)));

            let wm2 = writer.watermark();
            assert_eq!(wm2, Some(Offset(3)));

            writer.close().await.expect("close writer");
        }
    }

    #[tokio::test]
    async fn tail_from_start_sees_existing_and_new() {
        let writer: Writer<u64> = make_writer();

        // Initial data, committed.
        let mut tx = writer.transaction().expect("start tx");
        tx.put_blocking(&1).expect("put 1");
        tx.put_blocking(&2).expect("put 2");
        tx.put_blocking(&3).expect("put 3");
        let _ = tx.close_blocking().expect("close tx");

        // Start tail from the beginning.
        let mut tx = writer.transaction().expect("start tail tx");
        let tail_stream = tx.tail(TailFrom::Start);

        // Add more data and commit.
        tx.put_blocking(&4).expect("put 4");
        tx.put_blocking(&5).expect("put 5");
        let _ = tx.close_blocking().expect("close tx2");

        // Now the tail stream should be finite and complete after close.
        let items: Vec<(Offset, u64)> = tail_stream
            .try_collect()
            .await
            .expect("collect tail stream");

        assert_eq!(
            items,
            vec![
                (Offset(0), 1),
                (Offset(1), 2),
                (Offset(2), 3),
                (Offset(3), 4),
                (Offset(4), 5),
            ]
        );

        writer.close().await.expect("close writer");
    }

    #[tokio::test]
    async fn tail_from_head_sees_only_new() {
        let writer: Writer<String> = make_writer();

        // Initial committed data.
        let mut tx = writer.transaction().expect("start tx");
        tx.put_blocking(&"old1".to_string()).expect("put old1");
        tx.put_blocking(&"old2".to_string()).expect("put old2");
        let _ = tx.close_blocking().expect("close tx");

        // Start tail from head: should NOT replay old1/old2.
        let mut tx = writer.transaction().expect("start tx");
        let tail_stream = tx.tail(TailFrom::Head);

        // New committed data.
        tx.put_blocking(&"new1".to_string()).expect("put new1");
        tx.put_blocking(&"new2".to_string()).expect("put new2");
        let _ = tx.close_blocking().expect("close tx2");

        let items: Vec<(Offset, String)> = tail_stream
            .try_collect()
            .await
            .expect("collect tail stream");

        // Offsets 2 and 3 (after old1/old2)
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, Offset(2));
        assert_eq!(items[0].1, "new1");
        assert_eq!(items[1].0, Offset(3));
        assert_eq!(items[1].1, "new2");

        writer.close().await.expect("close writer");
    }

    #[tokio::test]
    async fn only_one_active_transaction() {
        let writer: Writer<u64> = make_writer();

        let _tx1 = writer.transaction().expect("start first tx");

        // Second transaction should fail with ActiveTransactionInProgress.
        match writer.transaction() {
            Ok(_) => panic!("transaction should fail"),
            Err(Error::ActiveTransactionInProgress) => {}
            Err(e) => panic!("expected ActiveTransactionInProgress, got {e:?}"),
        };

        writer.close().await.expect("close writer");
    }

    #[tokio::test]
    async fn flush_without_writes_is_noop() {
        let writer: Writer<u64> = make_writer();

        // Initially empty.
        assert_eq!(writer.watermark(), None);

        // Start a tx but don't write anything.
        let mut tx = writer.transaction().expect("start tx");
        let res = tx.flush_blocking().expect("flush empty tx");

        // No last_offset => no watermark advance.
        assert_eq!(res, None);
        assert_eq!(writer.watermark(), None);

        // Reader sees nothing.
        let reader = writer.reader();
        let items: Vec<(Offset, u64)> = reader
            .from(Offset::start())
            .try_collect()
            .await
            .expect("collect stream");
        assert!(items.is_empty(), "expected no items after empty flush");

        writer.close().await.expect("close writer");
    }

    #[tokio::test]
    async fn multiple_flushes_advance_watermark() {
        let writer: Writer<u64> = make_writer();

        let mut tx = writer.transaction().expect("start tx");

        // First write + flush.
        tx.put_blocking(&10).expect("put 10");
        let last = tx.flush_blocking().expect("first flush");
        assert_eq!(last, Some(Offset(0)));
        assert_eq!(writer.watermark(), Some(Offset(0)));

        // Second write + flush.
        tx.put_blocking(&20).expect("put 20");
        let last = tx.flush_blocking().expect("second flush");
        assert_eq!(last, Some(Offset(1)));
        assert_eq!(writer.watermark(), Some(Offset(1)));

        // Third write, only committed on close.
        tx.put_blocking(&30).expect("put 30");
        assert_eq!(writer.watermark(), Some(Offset(1)));

        let last = tx.close_blocking().expect("close tx");
        assert_eq!(last, Some(Offset(2)));
        assert_eq!(writer.watermark(), Some(Offset(2)));

        // Reader should see all three values in order.
        let reader = writer.reader();
        let items: Vec<(Offset, u64)> = reader
            .from(Offset::start())
            .try_collect()
            .await
            .expect("collect stream");
        assert_eq!(
            items,
            vec![(Offset(0), 10), (Offset(1), 20), (Offset(2), 30),]
        );

        writer.close().await.expect("close writer");
    }

    #[tokio::test]
    async fn from_past_end_is_empty() {
        let writer: Writer<u64> = make_writer();

        // Write and commit two items at offsets 0 and 1.
        let mut tx = writer.transaction().expect("start tx");
        tx.put_blocking(&100).expect("put 100");
        tx.put_blocking(&200).expect("put 200");
        let _ = tx.close_blocking().expect("close tx");

        assert_eq!(writer.watermark(), Some(Offset(1)));

        // Start reading from offset 5 (> 1): should be empty.
        let reader = writer.reader();
        let items: Vec<(Offset, u64)> = reader
            .from(Offset(5))
            .try_collect()
            .await
            .expect("collect stream");
        assert!(
            items.is_empty(),
            "expected empty stream when starting beyond watermark"
        );

        writer.close().await.expect("close writer");
    }

    #[tokio::test]
    async fn tail_from_offset_replays_subset_and_new() {
        let writer: Writer<u64> = make_writer();

        // Initial committed data: offsets 0..=4 with values 0..=4.
        {
            let mut tx = writer.transaction().expect("start tx");
            for i in 0u64..5u64 {
                tx.put_blocking(&i).expect("put initial");
            }
            let last = tx.close_blocking().expect("close tx");
            assert_eq!(last, Some(Offset(4)));
        }

        // Start a new transaction and tail from offset 2.
        let mut tx = writer.transaction().expect("start tail tx");
        let tail_stream = tx.tail(TailFrom::Offset(Offset(2)));

        // New committed data at offsets 5 and 6.
        tx.put_blocking(&100).expect("put 100");
        tx.put_blocking(&101).expect("put 101");
        let _ = tx.close_blocking().expect("close tail tx");

        let items: Vec<(Offset, u64)> = tail_stream
            .try_collect()
            .await
            .expect("collect tail stream");

        // Expect:
        //   existing: offsets 2,3,4 with values 2,3,4
        //   new:      offsets 5,6 with values 100,101
        assert_eq!(
            items,
            vec![
                (Offset(2), 2),
                (Offset(3), 3),
                (Offset(4), 4),
                (Offset(5), 100),
                (Offset(6), 101),
            ]
        );

        writer.close().await.expect("close writer");
    }

    #[tokio::test]
    async fn new_transaction_after_close_is_allowed() {
        let writer: Writer<u64> = make_writer();

        {
            let mut tx1 = writer.transaction().expect("start first tx");
            tx1.put_blocking(&1).expect("put 1");
            let _ = tx1.close_blocking().expect("close first tx");
        }

        // After closing, we should be able to start another transaction.
        let mut tx2 = writer.transaction().expect("start second tx");
        tx2.put_blocking(&2).expect("put 2");
        let last = tx2.close_blocking().expect("close second tx");

        assert_eq!(last, Some(Offset(1)));

        let reader = writer.reader();
        let items: Vec<(Offset, u64)> = reader
            .from(Offset::start())
            .try_collect()
            .await
            .expect("collect stream");
        assert_eq!(items, vec![(Offset(0), 1), (Offset(1), 2),]);

        writer.close().await.expect("close writer");
    }

    #[tokio::test]
    async fn tail_from_start_on_empty_log_sees_only_new() {
        let writer: Writer<u64> = make_writer();

        assert_eq!(writer.watermark(), None);

        // Start a tx solely to create the tail stream.
        let mut tx = writer.transaction().expect("start tx for tail");
        let tail_stream = tx.tail(TailFrom::Start);

        // Now write and commit some items; these are the first ever entries.
        tx.put_blocking(&10).expect("put 10");
        tx.put_blocking(&20).expect("put 20");
        tx.put_blocking(&30).expect("put 30");
        let _ = tx.close_blocking().expect("close tx");

        let items: Vec<(Offset, u64)> = tail_stream
            .try_collect()
            .await
            .expect("collect tail stream");

        assert_eq!(
            items,
            vec![(Offset(0), 10), (Offset(1), 20), (Offset(2), 30),]
        );

        writer.close().await.expect("close writer");
    }

    #[tokio::test]
    async fn log_in_order_read() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("db");
        const N: u64 = 10000;

        // First instance: write a lot of data and close
        {
            let writer: Writer<u64> =
                Writer::open(path.clone(), "test".to_string()).expect("create writer");

            let mut tx = writer.transaction().expect("start tx");
            for i in 0u64..N {
                tx.put_blocking(&i).expect("put");
            }

            let last = tx.close_blocking().expect("close tx");
            assert_eq!(last, Some(Offset(N - 1)));

            let wm = writer.watermark();
            assert_eq!(wm, Some(Offset(N - 1)));

            writer.close().await.expect("close writer");
        }

        // Second instance: reopen and verify recovery.
        {
            let writer: Writer<u64> =
                Writer::open(path.clone(), "test".to_string()).expect("reopen writer");

            // Watermark should be recovered.
            let wm = writer.watermark();
            assert_eq!(wm, Some(Offset(N - 1)));

            // Reading from the start should give us the same data.
            let reader = writer.reader();
            let stream = reader.from(Offset::start());
            let items: Vec<(Offset, u64)> =
                stream.try_collect().await.expect("collect after reopen");
            assert_eq!(items.len() as u64, N);
            for i in 0u64..N {
                assert_eq!(items[i as usize], (Offset(i), i));
            }

            // Next transaction should continue from offset N.
            let mut tx = writer.transaction().expect("start tx 2");
            tx.put_blocking(&N).expect("put N");
            let last = tx.close_blocking().expect("close tx 2");
            assert_eq!(last, Some(Offset(N)));

            let wm2 = writer.watermark();
            assert_eq!(wm2, Some(Offset(N)));

            writer.close().await.expect("close writer");
        }
    }
}
