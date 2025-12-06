use crate::db::error::DBError;
use crate::flightdeck;
use crate::flightdeck::tracer::Tracer;
use futures::StreamExt;
use futures_core::stream::BoxStream;
use parking_lot::RwLock;
use rocksdb::{BlockBasedOptions, Cache, DB, DBCompressionType, IteratorMode, Options};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task;
use tokio::task::JoinHandle;

const DEFAULT_BUFFER_SIZE: usize = 100;

pub enum UpsertAction<T> {
    NoChange,
    Change(T),
    Delete,
}

pub trait Upsert: Sync + Send + 'static {
    type K: Serialize + DeserializeOwned + Send + Sync + Clone + 'static;
    type V: Serialize + DeserializeOwned + Send + Sync + Clone + 'static;

    fn key(&self) -> Self::K;
    fn upsert(self, v: Option<Self::V>) -> UpsertAction<Self::V>;
}

#[derive(Clone)]
pub(crate) struct AlwaysUpsert<K, V>(pub(crate) K, pub(crate) V)
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;

impl<K, V> Upsert for AlwaysUpsert<K, V>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    type K = K;
    type V = V;

    fn key(&self) -> Self::K {
        self.0.clone()
    }

    fn upsert(self, _: Option<Self::V>) -> UpsertAction<Self::V> {
        UpsertAction::Change(self.1.clone())
    }
}

pub(crate) struct UpsertedValue<U, V> {
    pub(crate) upsert: U,
    pub(crate) previous_value: Option<V>,
}

pub(crate) struct DatabaseGuard {
    pub(crate) name: String,
    pub(crate) db: Option<DB>,
}

impl std::ops::Deref for DatabaseGuard {
    type Target = Option<DB>;
    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl std::ops::DerefMut for DatabaseGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.db
    }
}

impl DatabaseGuard {
    pub(crate) fn close_sync(&mut self) {
        let db = self.db.take();
        let name = self.name.clone();
        let tracer = Tracer::new_on(format!("KVStore({})::close", name));
        drop(db);
        tracer.measure();
    }
}

impl Drop for DatabaseGuard {
    fn drop(&mut self) {
        if let Some(_db) = self.db.take() {
            if cfg!(feature = "__kv-drop-dev-assert") {
                panic!("Use ::close instead of relying on the default Drop behaviour");
            } else {
                log::error!("Use ::close instead of relying on the default Drop behaviour");
                let tracer = Tracer::new_on(format!("KVStore({})::drop", self.name));
                drop(_db);
                tracer.measure();
            }
        }
    }
}

pub(crate) struct KVStore<K, V>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    db: Arc<RwLock<DatabaseGuard>>,
    name: String,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Clone for KVStore<K, V>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            name: self.name.clone(),
            _marker: PhantomData,
        }
    }
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

impl<K, V> KVStore<K, V>
where
    K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    pub(crate) async fn new(path: PathBuf, name: String) -> Result<Self, DBError> {
        task::spawn_blocking(move || {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let opts = make_options();
            let db = DB::open(&opts, &path)?;

            let guard_name = name.clone();
            let db = Arc::new(RwLock::new(DatabaseGuard {
                db: Some(db),
                name: guard_name,
            }));

            Ok(Self {
                db,
                name,
                _marker: PhantomData,
            })
        })
        .await?
    }

    pub(crate) async fn close(&self) -> Result<(), DBError> {
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let mut guard = db.write();
            guard.close_sync();
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn compact(&self) -> Result<(), DBError> {
        let tracer = Tracer::new_on(format!("KVStore({})::compact", self.name));
        let db = self.db.clone();
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

    pub(crate) fn stream(&self) -> BoxStream<'static, Result<(K, V), DBError>> {
        let db = self.db.clone();
        let name = format!("KVStore({})::stream", self.name);
        let (tx, rx) = flightdeck::tracked::mpsc_channel(name.clone(), DEFAULT_BUFFER_SIZE);

        task::spawn_blocking(move || {
            let work: Result<(), DBError> = (|| {
                let tracer = Tracer::new_on(format!("{}::acq", name));
                let guard = db.read();
                tracer.measure();

                let tracer = Tracer::new_on(format!("{}::iter", name));
                let db_ref = guard.as_ref().ok_or(DBError::AccessAfterDrop)?;
                for row in db_ref.iterator(IteratorMode::Start) {
                    let (key_bytes, value_bytes) = row?;
                    let key: K = bincode::deserialize(&key_bytes)?;
                    let value: V = bincode::deserialize(&value_bytes)?;
                    tx.blocking_send(Ok((key, value)))?;
                }
                tracer.measure();

                Ok(())
            })();

            if let Err(e) = work {
                log::error!("failed to stream {} from rocksdb: {}", name, e);
                let _ = tx.blocking_send(Err(e));
            }
        });

        rx.boxed()
    }

    pub(crate) async fn apply<E: From<DBError> + Send + Sync + 'static>(
        &self,
        s: BoxStream<'static, Result<(K, Option<V>), E>>,
    ) -> Result<u64, E> {
        let (tx, mut rx) = mpsc::channel::<Result<_, E>>(DEFAULT_BUFFER_SIZE);
        let db = self.db.clone();
        let name = self.name.clone();

        let bg = task::spawn_blocking(move || -> Result<u64, E> {
            let tracer = Tracer::new_on(format!("KVStore({})::apply::acq", name));
            let guard = db.read();
            tracer.measure();

            let tracer = Tracer::new_on(format!("KVStore({})::apply::open", name));
            let db_ref = guard.as_ref().ok_or(DBError::AccessAfterDrop)?;
            tracer.measure();

            let mut tracer = Tracer::new_off(format!("KVStore({})::apply", name));
            let mut count = 0u64;
            while let Some(item) = rx.blocking_recv() {
                let (key, value) = item?;
                tracer.on();

                let key_bytes = bincode::serialize(&key).map_err(Into::into)?;
                match value {
                    None => {
                        db_ref.delete(&key_bytes).map_err(Into::into)?;
                    }
                    Some(v) => {
                        let val_bytes = bincode::serialize(&v).map_err(Into::into)?;
                        db_ref.put(&key_bytes, &val_bytes).map_err(Into::into)?;
                    }
                }
                count += 1;
                tracer.off();
            }
            tracer.measure();

            let tracer = Tracer::new_on(format!("KVStore({})::upsert::commit", name));
            if count > 0 {
                db_ref.flush_wal(true).map_err(Into::into)?;
            }
            tracer.measure();

            Ok(count)
        });

        let mut s = s.boxed();
        while let Some(item) = s.next().await {
            if tx.send(item).await.is_err() {
                break;
            }
        }
        drop(tx);

        let count = bg.await.map_err(Into::into)??;
        Ok(count)
    }

    pub(crate) async fn upsert<U: Upsert<K = K, V = V>>(
        &self,
        s: BoxStream<'_, Result<U, DBError>>,
    ) -> Result<u64, DBError> {
        let (tx, mut rx) = mpsc::channel::<Result<_, DBError>>(DEFAULT_BUFFER_SIZE);
        let db = self.db.clone();
        let name = self.name.clone();

        let bg = task::spawn_blocking(move || -> Result<u64, DBError> {
            let tracer = Tracer::new_on(format!("KVStore({})::upsert::acq", name));
            let guard = db.read();
            tracer.measure();

            let tracer = Tracer::new_on(format!("KVStore({})::upsert::open", name));
            let db_ref = guard.as_ref().ok_or(DBError::AccessAfterDrop)?;
            tracer.measure();

            let mut tracer = Tracer::new_off(format!("KVStore({})::upsert", name));
            let mut count = 0u64;

            while let Some(item) = rx.blocking_recv() {
                let item: U = item?;
                let key = item.key();
                tracer.on();

                let key_bytes = bincode::serialize(&key)?;
                let existing = db_ref.get(&key_bytes)?;

                let mut delete_key = false;

                match existing {
                    Some(bytes) => {
                        let current: V = bincode::deserialize(&bytes)?;
                        match item.upsert(Some(current)) {
                            UpsertAction::Change(v) => {
                                let v_bytes = bincode::serialize(&v)?;
                                db_ref.put(&key_bytes, &v_bytes)?;
                                count += 1;
                            }
                            UpsertAction::Delete => {
                                delete_key = true;
                            }
                            UpsertAction::NoChange => {}
                        }
                    }
                    None => {
                        if let UpsertAction::Change(v) = item.upsert(None) {
                            let v_bytes = bincode::serialize(&v)?;
                            db_ref.put(&key_bytes, &v_bytes)?;
                            count += 1;
                        }
                    }
                }

                if delete_key {
                    db_ref.delete(&key_bytes)?;
                    count += 1;
                }

                tracer.off();
            }
            tracer.measure();

            let tracer = Tracer::new_on(format!("KVStore({})::upsert::commit", name));
            if count > 0 {
                db_ref.flush_wal(true)?;
            }
            tracer.measure();

            Ok(count)
        });

        let mut s = s.boxed();
        while let Some(item) = s.next().await {
            if tx.send(item).await.is_err() {
                break;
            }
        }
        drop(tx);

        let count = bg.await??;
        Ok(count)
    }

    pub(crate) fn left_join<IK, KF, E: From<DBError> + Debug + Send + Sync + 'static>(
        &self,
        s: BoxStream<'static, Result<IK, E>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<V>), E>>
    where
        KF: Fn(IK) -> K + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        let (tx_in, mut rx_in) = mpsc::channel::<Result<_, E>>(DEFAULT_BUFFER_SIZE);
        let (tx, rx) = flightdeck::tracked::mpsc_channel::<Result<_, E>>(
            format!("KVStore({})::left_join", self.name),
            DEFAULT_BUFFER_SIZE,
        );

        let db = self.db.clone();
        let name = self.name.clone();
        task::spawn_blocking(move || {
            let work: Result<(), E> = (|| {
                let tracer = Tracer::new_on(format!("KVStore({})::left_join::acq", name));
                let guard = db.read();
                tracer.measure();

                let tracer = Tracer::new_on(format!("KVStore({})::left_join::open", name));
                let db_ref = guard.as_ref().ok_or(DBError::AccessAfterDrop)?;
                tracer.measure();

                let mut tracer = Tracer::new_off(format!("KVStore({})::left_join", name));

                while let Some(key_like) = rx_in.blocking_recv() {
                    let key_like: IK = key_like?;
                    let key: K = key_func(key_like.clone());
                    tracer.on();

                    let key_bytes = bincode::serialize(&key).map_err(Into::into)?;
                    let blob = db_ref.get(&key_bytes).map_err(Into::<DBError>::into)?;
                    let value: Option<V> = match blob {
                        Some(bytes) => Some(bincode::deserialize(&bytes).map_err(Into::into)?),
                        None => None,
                    };

                    tracer.off();
                    tx.blocking_send(Ok((key_like, value)))
                        .map_err(Into::<DBError>::into)?;
                }

                tracer.measure();
                Ok(())
            })();

            if let Err(e) = work {
                log::error!("failed to lookup({}) in rocksdb: {:?}", name, e);
                let _ = tx.blocking_send(Err(e));
            }
        });

        let name = self.name.clone();
        tokio::spawn(async move {
            let mut s = s.boxed();
            let mut count = 0u64;
            while let Some(item) = s.next().await {
                if let Err(e) = tx_in.send(item).await {
                    log::error!("failed to lookup({}) in rocksdb: {}", name, e);
                    return Err::<_, DBError>(e.into());
                }
                count += 1;
            }
            drop(tx_in);
            Ok::<u64, DBError>(count)
        });

        rx.boxed()
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn streaming_upsert<
        U: Upsert<K = K, V = V> + Clone,
        E: From<DBError> + Send + Sync + 'static,
    >(
        &self,
        s: BoxStream<'static, Result<U, E>>,
    ) -> (
        BoxStream<'static, Result<UpsertedValue<U, V>, E>>,
        JoinHandle<Result<u64, E>>,
    ) {
        let db = self.db.clone();
        let name = self.name.clone();

        let (tx_in, mut rx_in) = mpsc::channel::<Result<_, E>>(DEFAULT_BUFFER_SIZE);
        let (tx, rx) = flightdeck::tracked::mpsc_channel(
            format!("RocksDB::streaming_upsert({})", name),
            DEFAULT_BUFFER_SIZE,
        );

        let bg = task::spawn_blocking(move || -> Result<u64, DBError> {
            let tracer = Tracer::new_on(format!("KVStore({})::streaming_upsert::acq", name));
            let guard = db.read();
            tracer.measure();

            let tracer = Tracer::new_on(format!("KVStore({})::streaming_upsert::open", name));
            let db_ref = guard.as_ref().ok_or(DBError::AccessAfterDrop)?;
            tracer.measure();

            let mut tracer = Tracer::new_off(format!("KVStore({})::streaming_upsert", name));
            let mut count = 0u64;

            while let Some(upsert) = rx_in.blocking_recv() {
                let upsert: U = match upsert {
                    Ok(i) => i,
                    Err(e) => {
                        tx.blocking_send(Err(e))?;
                        continue;
                    }
                };

                let key = upsert.key();
                tracer.on();

                let key_bytes = bincode::serialize(&key)?;
                let existing_bytes = db_ref.get(&key_bytes)?;

                let uv = match existing_bytes {
                    Some(bytes) => {
                        let u = upsert.clone();
                        let current: V = bincode::deserialize(&bytes)?;
                        match upsert.upsert(Some(current.clone())) {
                            UpsertAction::Change(v_new) => {
                                let v_bytes = bincode::serialize(&v_new)?;
                                db_ref.put(&key_bytes, &v_bytes)?;
                                count += 1;
                            }
                            UpsertAction::Delete => {
                                db_ref.delete(&key_bytes)?;
                                count += 1;
                            }
                            UpsertAction::NoChange => {}
                        };
                        UpsertedValue::<U, V> {
                            upsert: u,
                            previous_value: Some(current.clone()),
                        }
                    }
                    None => {
                        let u = upsert.clone();
                        if let UpsertAction::Change(v_new) = upsert.upsert(None) {
                            let v_bytes = bincode::serialize(&v_new)?;
                            db_ref.put(&key_bytes, &v_bytes)?;
                            count += 1;
                        };
                        UpsertedValue {
                            upsert: u,
                            previous_value: None,
                        }
                    }
                };

                tracer.off();
                tx.blocking_send(Ok(uv))?;
            }
            tracer.measure();

            let tracer = Tracer::new_on(format!("KVStore({})::upsert::commit", name));
            if count > 0 {
                db_ref.flush_wal(true)?;
            }
            tracer.measure();

            Ok(count)
        });

        let name = self.name.clone();
        let bg = tokio::spawn(async move {
            let mut s = s.boxed();
            let mut count = 0u64;
            while let Some(item) = s.next().await {
                if let Err(e) = tx_in.send(item).await {
                    log::error!("failed to streaming_upsert({}) in rocksdb: {}", name, e);
                    return Err(Into::<DBError>::into(e).into());
                }
                count += 1;
            }
            drop(tx_in);
            bg.await.map_err(Into::into)??;
            Ok(count)
        });

        (rx.boxed(), bg)
    }
}
