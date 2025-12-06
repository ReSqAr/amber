use futures::StreamExt;
use futures::stream::{self, BoxStream};
use rocksdb::{BlockBasedOptions, Cache, DB, DBCompressionType, Direction, IteratorMode, Options};
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
use tokio_stream::wrappers::ReceiverStream;

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
    Offset(Offset),
    Head,
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
    Serde(#[from] bincode::Error),

    #[error("failed to convert key")]
    KeyConversion,

    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
}

struct Inner {
    db: Arc<DB>,
    next_offset: AtomicU64,
    // Last durably committed offset (None = empty).
    watermark: RwLock<Option<Offset>>,
    watermark_tx: watch::Sender<Option<Offset>>,
    active_tx: RwLock<bool>,
}

impl Inner {
    fn current_watermark(&self) -> Result<Option<Offset>, Error> {
        self.watermark
            .read()
            .map(|g| *g)
            .map_err(|_| Error::Poisoned)
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
    /// Last offset written by this transaction (if any).
    last_offset: Option<Offset>,
    tail_cancels: Mutex<Vec<tokio::sync::oneshot::Sender<()>>>,
    closed: bool,
    _marker: PhantomData<V>,
}

fn make_options() -> Options {
    let mut opts = Options::default();

    // General
    opts.create_if_missing(true);
    opts.increase_parallelism(num_cpus::get().try_into().unwrap_or(4));
    opts.optimize_level_style_compaction(64 * 1024 * 1024);

    // Block-based table
    let mut block_opts = BlockBasedOptions::default();

    // Bloom filter for point lookups
    block_opts.set_bloom_filter(10.0, false);

    // Cache data/index/filter blocks in memory.
    let cache = Cache::new_lru_cache(128 * 1024 * 1024);
    block_opts.set_block_cache(&cache);
    block_opts.set_cache_index_and_filter_blocks(true);

    // Reasonable block size
    block_opts.set_block_size(16 * 1024);

    opts.set_block_based_table_factory(&block_opts);

    // Compression: LZ4 is fast and usually good tradeoff.
    opts.set_compression_type(DBCompressionType::Lz4);

    // Dynamic level bytes works well for most workloads
    opts.set_level_compaction_dynamic_level_bytes(true);

    // Memtable / write buffers (see next section)
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.set_max_write_buffer_number(3);
    opts.set_min_write_buffer_number_to_merge(1);

    // Background threads for compaction/flush
    opts.set_max_background_jobs(4);

    // Smooth out I/O
    opts.set_bytes_per_sync(10 * 1024 * 1024); // fsync WAL every ~10MB
    opts.set_wal_bytes_per_sync(512 * 1024);

    opts
}

impl<V> Writer<V>
where
    V: Serialize + DeserializeOwned + Send + 'static,
{
    pub fn open(path: PathBuf) -> Result<Self, Error> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let opts = make_options();
        let db = DB::open(&opts, &path)?;

        let max_offset: Option<Offset> = {
            let mut iter = db.iterator(IteratorMode::End);
            if let Some(row) = iter.next() {
                let (k, _) = row?;
                if k.len() == 8 {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&k);
                    Some(Offset(u64::from_be_bytes(buf)))
                } else {
                    return Err(Error::KeyConversion);
                }
            } else {
                None
            }
        };

        let next_offset = match max_offset {
            Some(o) => AtomicU64::new(o.0 + 1),
            None => AtomicU64::new(0),
        };
        let watermark = RwLock::new(max_offset);
        let (watermark_tx, _) = watch::channel(max_offset);

        let db = Arc::new(db);
        let inner = Inner {
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

    pub fn watermark(&self) -> Result<Option<Offset>, Error> {
        self.inner.current_watermark()
    }

    pub fn reader(&self) -> Reader<V> {
        Reader {
            inner: Arc::clone(&self.inner),
            _marker: PhantomData,
        }
    }

    pub fn transaction(&self) -> Result<Transaction<V>, Error> {
        let mut guard = self.inner.active_tx.write().map_err(|_| Error::Poisoned)?;

        if *guard {
            return Err(Error::ActiveTransactionInProgress);
        }
        *guard = true;

        Ok(Transaction {
            inner: Arc::clone(&self.inner),
            last_offset: None,
            tail_cancels: Mutex::new(Vec::new()),
            closed: false,
            _marker: PhantomData,
        })
    }
}

impl<V> Transaction<V>
where
    V: Serialize + DeserializeOwned + Send + 'static,
{
    pub fn put(&mut self, v: &V) -> Result<(), Error> {
        if self.closed {
            return Err(Error::ClosedTransaction);
        }

        let offset_u64 = self.inner.next_offset.fetch_add(1, Ordering::SeqCst);
        let offset = Offset(offset_u64);

        let key = offset_u64.to_be_bytes();
        let bytes = bincode::serialize(v)?;

        self.inner.db.put(key, &bytes)?;
        self.last_offset = Some(offset);

        Ok(())
    }

    pub fn flush(&mut self) -> Result<Option<Offset>, Error> {
        self.persist_and_publish()
    }

    pub fn close(mut self) -> Result<Option<Offset>, Error> {
        let res = self.persist_and_publish();
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

    fn persist_and_publish(&mut self) -> Result<Option<Offset>, Error> {
        let Some(offset) = self.last_offset else {
            return Ok(None); // nothing written in this tx
        };

        self.inner.db.flush_wal(true)?;

        // Advance in-memory watermark.
        {
            let mut guard = self.inner.watermark.write().map_err(|_| Error::Poisoned)?;
            *guard = Some(offset);
        }

        let _ = self.inner.watermark_tx.send(Some(offset));

        Ok(Some(offset))
    }

    pub fn tail(
        &self,
        from: impl Into<TailFrom>,
    ) -> BoxStream<'static, Result<(Offset, V), Error>> {
        let db = Arc::clone(&self.inner.db);
        let mut watermark_rx = self.inner.watermark_tx.subscribe();
        let (tx, rx) = mpsc::channel::<Result<(Offset, V), Error>>(128);

        let watermark_snapshot = self.inner.current_watermark().ok().flatten();

        let (start_offset, include_existing) = match from.into() {
            TailFrom::Start => (Offset::start(), true),
            TailFrom::Offset(o) => (o, true),
            TailFrom::Head => {
                let start = watermark_snapshot
                    .map(|w| w.increment())
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
                let db_cl = db.clone();
                let tx_cl = tx.clone();
                let from_local = next;
                let to_local = wm;

                if let Err(join_err) = tokio::task::spawn_blocking(move || {
                    pump_range_blocking::<V>(db_cl, from_local, to_local, tx_cl);
                })
                .await
                {
                    panic!("spawn_blocking failed: {:?}", join_err);
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
                        if let Some(wm) = new_wm && wm.0 >= next.0 {
                            let db_cl = db.clone();
                            let tx_cl = tx.clone();
                            let from_local = next;
                            let to_local = wm;

                            if let Err(join_err) = tokio::task::spawn_blocking(move || {
                                pump_range_blocking::<V>(db_cl, from_local, to_local, tx_cl);
                            }).await {
                                panic!("spawn_blocking failed: {:?}", join_err);
                            }
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
                        if let Some(wm) = new_wm {
                            if wm.0 < next.0 {
                                continue;
                            }

                            let db_cl = db.clone();
                            let tx_cl = tx.clone();
                            let from_local = next;
                            let to_local = wm;

                            if let Err(join_err) = tokio::task::spawn_blocking(move || {
                                pump_range_blocking::<V>(db_cl, from_local, to_local, tx_cl);
                            }).await {
                                panic!("spawn_blocking failed: {:?}", join_err);
                            }

                            next = wm.increment();
                        }
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
    pub fn watermark(&self) -> Result<Option<Offset>, Error> {
        self.inner.current_watermark()
    }
    pub fn from(&self, from: Offset) -> BoxStream<'static, Result<(Offset, V), Error>> {
        let upper = match self.inner.current_watermark() {
            Ok(Some(w)) if from.0 <= w.0 => w,
            Ok(Some(_)) | Ok(None) => return stream::empty().boxed(),
            Err(e) => return stream::iter([Err(e)]).boxed(),
        };

        let db = Arc::clone(&self.inner.db);
        let (tx, rx) = mpsc::channel::<Result<(Offset, V), Error>>(128);

        tokio::task::spawn_blocking(move || {
            pump_range_blocking::<V>(db, from, upper, tx);
        });

        ReceiverStream::new(rx).boxed()
    }
}

fn pump_range_blocking<V>(
    db: Arc<DB>,
    from: Offset,
    to: Offset,
    tx: mpsc::Sender<Result<(Offset, V), Error>>,
) where
    V: DeserializeOwned + Send + 'static,
{
    let start_key = from.0.to_be_bytes();
    let iter = db.iterator(IteratorMode::From(&start_key, Direction::Forward));

    for row in iter {
        let (k, v) = match row {
            Ok((k, v)) => (k, v),
            Err(e) => {
                let _ = tx.blocking_send(Err(Error::Rocksdb(e)));
                break;
            }
        };

        if k.len() != 8 {
            let _ = tx.blocking_send(Err(Error::KeyConversion));
            break;
        }

        let mut buf = [0u8; 8];
        buf.copy_from_slice(&k);
        let off_u64 = u64::from_be_bytes(buf);

        if off_u64 < from.0 {
            continue;
        }
        if off_u64 > to.0 {
            break;
        }

        let value = match bincode::deserialize::<V>(&v) {
            Ok(v) => v,
            Err(e) => {
                if let Err(send_err) = tx.blocking_send(Err(Error::Serde(e))) {
                    panic!("send error: {:?}", send_err);
                }
                break;
            }
        };

        if let Err(send_err) = tx.blocking_send(Ok((Offset(off_u64), value))) {
            panic!("send error: {:?}", send_err);
        }
    }
    // tx dropped by caller when stream ends.
}

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

        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = Arc::new(DB::open(&opts, path).expect("open db"));

        let (watermark_tx, _watermark_rx) = watch::channel(None);

        let inner = Arc::new(Inner {
            db,
            next_offset: AtomicU64::new(0),
            watermark: RwLock::new(None),
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
        tx.put(&"one".to_string()).expect("put one");
        tx.put(&"two".to_string()).expect("put two");
        tx.put(&"three".to_string()).expect("put three");

        // Close (fsync + publish watermark).
        let last = tx.close().expect("close transaction");
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
    }

    #[tokio::test]
    async fn from_respects_watermark() {
        let writer: Writer<u64> = make_writer();

        // Write some items, but only commit part of them.
        let mut tx = writer.transaction().expect("start transaction");
        tx.put(&10).expect("put 10");
        tx.put(&20).expect("put 20");

        // First flush: watermark at offset 1
        let last = tx.flush().expect("flush tx");
        assert_eq!(last, Some(Offset(1)));

        // Add one more unflushed write
        tx.put(&30).expect("put 30 (unflushed)");

        // Now read from offset 0; we should only see the first two.
        let reader = writer.reader();
        let stream = reader.from(Offset::start());
        let items: Vec<(Offset, u64)> = stream.try_collect().await.expect("collect stream");

        assert_eq!(items.len(), 2);
        assert_eq!(items[0], (Offset(0), 10));
        assert_eq!(items[1], (Offset(1), 20));
    }

    #[tokio::test]
    async fn writer_new_recovers_watermark_and_offsets() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("db");

        // First instance: write some data and close.
        {
            let writer: Writer<String> = Writer::open(path.clone()).expect("create writer");

            let mut tx = writer.transaction().expect("start tx");
            tx.put(&"alpha".to_string()).expect("put alpha");
            tx.put(&"beta".to_string()).expect("put beta");
            tx.put(&"gamma".to_string()).expect("put gamma");

            let last = tx.close().expect("close tx");
            assert_eq!(last, Some(Offset(2)));

            let wm = writer.watermark().expect("wm ok");
            assert_eq!(wm, Some(Offset(2)));
        }

        // Second instance: reopen and verify recovery.
        {
            let writer: Writer<String> = Writer::open(path.clone()).expect("reopen writer");

            // Watermark should be recovered.
            let wm = writer.watermark().expect("wm ok");
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
            tx.put(&"delta".to_string()).expect("put delta");
            let last = tx.close().expect("close tx 2");
            assert_eq!(last, Some(Offset(3)));

            let wm2 = writer.watermark().expect("wm ok 2");
            assert_eq!(wm2, Some(Offset(3)));
        }
    }

    #[tokio::test]
    async fn tail_from_start_sees_existing_and_new() {
        let writer: Writer<u64> = make_writer();

        // Initial data, committed.
        let mut tx = writer.transaction().expect("start tx");
        tx.put(&1).expect("put 1");
        tx.put(&2).expect("put 2");
        tx.put(&3).expect("put 3");
        let _ = tx.close().expect("close tx");

        // Start tail from the beginning.
        let mut tx = writer.transaction().expect("start tail tx");
        let tail_stream = tx.tail(TailFrom::Start);

        // Add more data and commit.
        tx.put(&4).expect("put 4");
        tx.put(&5).expect("put 5");
        let _ = tx.close().expect("close tx2");

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
    }
    #[tokio::test]
    async fn tail_from_head_sees_only_new() {
        let writer: Writer<String> = make_writer();

        // Initial committed data.
        let mut tx = writer.transaction().expect("start tx");
        tx.put(&"old1".to_string()).expect("put old1");
        tx.put(&"old2".to_string()).expect("put old2");
        let _ = tx.close().expect("close tx");

        // Start tail from head: should NOT replay old1/old2.
        let mut tx = writer.transaction().expect("start tx");
        let tail_stream = tx.tail(TailFrom::Head);

        // New committed data.
        tx.put(&"new1".to_string()).expect("put new1");
        tx.put(&"new2".to_string()).expect("put new2");
        let _ = tx.close().expect("close tx2");

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
    }

    #[tokio::test]
    async fn flush_without_writes_is_noop() {
        let writer: Writer<u64> = make_writer();

        // Initially empty.
        assert_eq!(writer.watermark().expect("wm ok at start"), None);

        // Start a tx but don't write anything.
        let mut tx = writer.transaction().expect("start tx");
        let res = tx.flush().expect("flush empty tx");

        // No last_offset => no watermark advance.
        assert_eq!(res, None);
        assert_eq!(writer.watermark().expect("wm ok after flush"), None);

        // Reader sees nothing.
        let reader = writer.reader();
        let items: Vec<(Offset, u64)> = reader
            .from(Offset::start())
            .try_collect()
            .await
            .expect("collect stream");
        assert!(items.is_empty(), "expected no items after empty flush");
    }

    #[tokio::test]
    async fn multiple_flushes_advance_watermark() {
        let writer: Writer<u64> = make_writer();

        let mut tx = writer.transaction().expect("start tx");

        // First write + flush.
        tx.put(&10).expect("put 10");
        let last = tx.flush().expect("first flush");
        assert_eq!(last, Some(Offset(0)));
        assert_eq!(
            writer.watermark().expect("wm after first flush"),
            Some(Offset(0))
        );

        // Second write + flush.
        tx.put(&20).expect("put 20");
        let last = tx.flush().expect("second flush");
        assert_eq!(last, Some(Offset(1)));
        assert_eq!(
            writer.watermark().expect("wm after second flush"),
            Some(Offset(1))
        );

        // Third write, only committed on close.
        tx.put(&30).expect("put 30");
        assert_eq!(
            writer.watermark().expect("wm before close"),
            Some(Offset(1))
        );

        let last = tx.close().expect("close tx");
        assert_eq!(last, Some(Offset(2)));
        assert_eq!(writer.watermark().expect("wm after close"), Some(Offset(2)));

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
    }

    #[tokio::test]
    async fn from_past_end_is_empty() {
        let writer: Writer<u64> = make_writer();

        // Write and commit two items at offsets 0 and 1.
        let mut tx = writer.transaction().expect("start tx");
        tx.put(&100).expect("put 100");
        tx.put(&200).expect("put 200");
        let _ = tx.close().expect("close tx");

        assert_eq!(writer.watermark().expect("wm ok"), Some(Offset(1)));

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
    }

    #[tokio::test]
    async fn tail_from_offset_replays_subset_and_new() {
        let writer: Writer<u64> = make_writer();

        // Initial committed data: offsets 0..=4 with values 0..=4.
        {
            let mut tx = writer.transaction().expect("start tx");
            for i in 0u64..5u64 {
                tx.put(&i).expect("put initial");
            }
            let last = tx.close().expect("close tx");
            assert_eq!(last, Some(Offset(4)));
        }

        // Start a new transaction and tail from offset 2.
        let mut tx = writer.transaction().expect("start tail tx");
        let tail_stream = tx.tail(TailFrom::Offset(Offset(2)));

        // New committed data at offsets 5 and 6.
        tx.put(&100).expect("put 100");
        tx.put(&101).expect("put 101");
        let _ = tx.close().expect("close tail tx");

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
    }

    #[tokio::test]
    async fn new_transaction_after_close_is_allowed() {
        let writer: Writer<u64> = make_writer();

        {
            let mut tx1 = writer.transaction().expect("start first tx");
            tx1.put(&1).expect("put 1");
            let _ = tx1.close().expect("close first tx");
        }

        // After closing, we should be able to start another transaction.
        let mut tx2 = writer.transaction().expect("start second tx");
        tx2.put(&2).expect("put 2");
        let last = tx2.close().expect("close second tx");

        assert_eq!(last, Some(Offset(1)));

        let reader = writer.reader();
        let items: Vec<(Offset, u64)> = reader
            .from(Offset::start())
            .try_collect()
            .await
            .expect("collect stream");
        assert_eq!(items, vec![(Offset(0), 1), (Offset(1), 2),]);
    }

    #[tokio::test]
    async fn tail_from_start_on_empty_log_sees_only_new() {
        let writer: Writer<u64> = make_writer();

        assert_eq!(writer.watermark().expect("wm ok"), None);

        // Start a tx solely to create the tail stream.
        let mut tx = writer.transaction().expect("start tx for tail");
        let tail_stream = tx.tail(TailFrom::Start);

        // Now write and commit some items; these are the first ever entries.
        tx.put(&10).expect("put 10");
        tx.put(&20).expect("put 20");
        tx.put(&30).expect("put 30");
        let _ = tx.close().expect("close tx");

        let items: Vec<(Offset, u64)> = tail_stream
            .try_collect()
            .await
            .expect("collect tail stream");

        assert_eq!(
            items,
            vec![(Offset(0), 10), (Offset(1), 20), (Offset(2), 30),]
        );
    }
}
