use crate::db::error::DBError;
use crate::flightdeck;
use crate::flightdeck::tracer::Tracer;
use futures::StreamExt;
use futures_core::stream::BoxStream;
use parking_lot::RwLock;
use redb::{Database, Key, ReadableDatabase, TableDefinition, TableHandle, Value};
use std::borrow::Borrow;
use std::fmt::Debug;
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
    type K: Key + for<'a> Borrow<<<Self as Upsert>::K as Value>::SelfType<'a>> + Send + Sync + Clone;
    type V: Value + for<'a> Borrow<<<Self as Upsert>::V as Value>::SelfType<'a>> + Send + Sync;

    fn key(&self) -> Self::K;
    fn upsert(self, v: Option<<Self::V as Value>::SelfType<'_>>) -> UpsertAction<Self::V>;
}

#[derive(Clone)]
pub(crate) struct AlwaysUpsert<K, V>(pub(crate) K, pub(crate) V)
where
    K: Key + for<'a> Borrow<K::SelfType<'a>> + Clone + Send + Sync + 'static,
    V: Value + for<'a> Borrow<V::SelfType<'a>> + Clone + Send + Sync + 'static;

impl<K, V> Upsert for AlwaysUpsert<K, V>
where
    K: Key + for<'a> Borrow<K::SelfType<'a>> + Clone + Send + Sync + 'static,
    V: Value + for<'a> Borrow<V::SelfType<'a>> + Clone + Send + Sync + 'static,
{
    type K = K;
    type V = V;

    fn key(&self) -> Self::K {
        self.0.clone()
    }

    fn upsert(self, _: Option<<Self::V as Value>::SelfType<'_>>) -> UpsertAction<Self::V> {
        UpsertAction::Change(self.1.clone())
    }
}

pub(crate) struct UpsertedValue<U, V> {
    pub(crate) upsert: U,
    pub(crate) previous_value: Option<V>,
}

pub(crate) struct DatabaseGuard {
    pub(crate) name: String,
    pub(crate) db: Option<Database>,
}

impl std::ops::Deref for DatabaseGuard {
    type Target = Option<Database>;
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

pub(crate) struct KVStore<K, V, KO, VO>
where
    K: Key + 'static,
    V: Value + 'static,
    for<'a> K::SelfType<'a>: ToOwned<Owned = KO>,
    for<'a> V::SelfType<'a>: ToOwned<Owned = VO>,
{
    db: Arc<RwLock<DatabaseGuard>>,
    table: TableDefinition<'static, K, V>,
}

impl<K, V, KO, VO> Clone for KVStore<K, V, KO, VO>
where
    K: Key + 'static,
    V: Value + 'static,
    for<'a> K::SelfType<'a>: ToOwned<Owned = KO>,
    for<'a> V::SelfType<'a>: ToOwned<Owned = VO>,
{
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            table: self.table,
        }
    }
}

impl<K, V, KO, VO> KVStore<K, V, KO, VO>
where
    K: Key + Clone + for<'a> Borrow<<K as Value>::SelfType<'a>> + Send + Sync + 'static,
    V: Value + Clone + for<'a> Borrow<<V as Value>::SelfType<'a>> + Send + Sync + 'static,
    for<'a> K::SelfType<'a>: ToOwned<Owned = KO>,
    for<'a> V::SelfType<'a>: ToOwned<Owned = VO>,
    KO: Send + Sync + Clone + 'static,
    VO: Send + Sync + Clone + 'static,
{
    pub(crate) async fn new(
        path: PathBuf,
        table: TableDefinition<'static, K, V>,
    ) -> Result<Self, DBError> {
        task::spawn_blocking(move || {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let db = if path.exists() {
                Database::open(path)?
            } else {
                let db = Database::create(path)?;
                {
                    let txn = db.begin_write()?;
                    {
                        let _ = txn.open_table(table)?;
                    }
                    txn.commit()?;
                }
                db
            };

            let db = Arc::new(RwLock::new(DatabaseGuard {
                db: Some(db),
                name: table.name().to_owned(),
            }));
            Ok(Self { db, table })
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
        let tracer = Tracer::new_on(format!("KVStore({})::compact", self.table.name()));
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let mut guard = db.write();
            Ok::<_, DBError>(guard.as_mut().ok_or(DBError::AccessAfterDrop)?.compact()?)
        })
        .await??;
        tracer.measure();
        Ok(())
    }

    pub(crate) fn stream(&self) -> BoxStream<'static, Result<(KO, VO), DBError>> {
        let table = self.table;
        let db = self.db.clone();
        let name = format!("KVStore({})::stream", table.name());
        let (tx, rx) = flightdeck::tracked::mpsc_channel(name, DEFAULT_BUFFER_SIZE);
        task::spawn_blocking(move || {
            let work: Result<(), DBError> = (|| {
                let tracer = Tracer::new_on(format!("KVStore({})::stream::acq", table.name()));
                let txn = db
                    .read()
                    .as_ref()
                    .ok_or(DBError::AccessAfterDrop)?
                    .begin_read()?;
                tracer.measure();

                let table = txn.open_table(table)?;
                for item in table.range::<K>(..)? {
                    let (key, value) = item?;
                    let key_owned: KO = key.value().to_owned();
                    let value_owned: VO = value.value().to_owned();
                    tx.blocking_send(Ok((key_owned, value_owned)))?;
                }

                let tracer = Tracer::new_on(format!("KVStore({})::stream::close", table.name()));
                drop(table);
                txn.close().map_err(Into::<DBError>::into)?;
                tracer.measure();

                Ok(())
            })();

            if let Err(e) = work {
                log::error!("failed to stream {} from redb: {}", table.name(), e);
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
        let table = self.table;

        let bg = task::spawn_blocking(move || -> Result<u64, E> {
            let tracer = Tracer::new_on(format!("KVStore({})::apply::acq", table.name()));
            let guard = db.read();
            tracer.measure();

            let tracer = Tracer::new_on(format!("KVStore({})::apply::open", table.name()));
            let txn = guard
                .as_ref()
                .ok_or(DBError::AccessAfterDrop)?
                .begin_write()
                .map_err(Into::into)?;
            let mut table = txn.open_table(table).map_err(Into::into)?;
            tracer.measure();

            let mut tracer = Tracer::new_off(format!("KVStore({})::apply", table.name()));
            let mut count = 0u64;
            {
                while let Some(item) = rx.blocking_recv() {
                    count += 1;
                    let (key, value) = item?;
                    tracer.on();
                    match value {
                        None => table.remove(key).map_err(Into::into)?,
                        Some(v) => table.insert(key, v).map_err(Into::into)?,
                    };
                    tracer.off();
                }
            }
            tracer.measure();

            let tracer = Tracer::new_on(format!("KVStore({})::apply::commit", table.name()));
            drop(table);
            if count > 0 {
                txn.commit().map_err(Into::into)?;
            } else {
                txn.abort().map_err(Into::into)?;
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
        let table = self.table;
        let db = self.db.clone();
        let bg = task::spawn_blocking(move || -> Result<u64, DBError> {
            let tracer = Tracer::new_on(format!("KVStore({})::upsert::acq", table.name()));
            let guard = db.read();
            tracer.measure();

            let tracer = Tracer::new_on(format!("KVStore({})::upsert::open", table.name()));
            let txn = guard
                .as_ref()
                .ok_or(DBError::AccessAfterDrop)?
                .begin_write()?;
            let mut table = txn.open_table(table)?;
            tracer.measure();

            let mut tracer = Tracer::new_off(format!("KVStore({})::upsert", table.name()));
            let mut count = 0u64;
            {
                while let Some(item) = rx.blocking_recv() {
                    let item: U = item?;
                    let key = item.key();
                    tracer.on();
                    let delete_key = match table.get_mut(item.key())? {
                        Some(mut guard) => match item.upsert(Some(guard.value())) {
                            UpsertAction::Change(v) => {
                                count += 1;
                                guard.insert(v)?;
                                false
                            }
                            UpsertAction::Delete => true,
                            UpsertAction::NoChange => false,
                        },
                        None => {
                            if let UpsertAction::Change(v) = item.upsert(None) {
                                count += 1;
                                table.insert(key.clone(), v)?;
                            }
                            false
                        }
                    };
                    if delete_key {
                        count += 1;
                        table.remove(key)?;
                    }
                    tracer.off();
                }
            }
            tracer.measure();

            let tracer = Tracer::new_on(format!("KVStore({})::upsert::commit", table.name()));
            drop(table);
            if count > 0 {
                txn.commit()?;
            } else {
                txn.abort()?;
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
    ) -> BoxStream<'static, Result<(IK, Option<VO>), E>>
    where
        KF: Fn(IK) -> K + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        let table = self.table;
        let db = self.db.clone();
        let (tx_in, mut rx_in) = mpsc::channel::<Result<_, E>>(DEFAULT_BUFFER_SIZE);
        let (tx, rx) = flightdeck::tracked::mpsc_channel::<Result<_, E>>(
            format!("KVStore({})::left_join", table.name()),
            DEFAULT_BUFFER_SIZE,
        );
        task::spawn_blocking(move || {
            let work: Result<(), E> = (|| {
                let tracer = Tracer::new_on(format!("KVStore({})::left_join::acq", table.name()));
                let guard = db.read();
                tracer.measure();

                let tracer =
                    Tracer::new_on(format!("KVStore({})::left_join::open", table.name()));
                let txn = guard
                    .as_ref()
                    .ok_or(DBError::AccessAfterDrop)?
                    .begin_read()
                    .map_err(Into::<DBError>::into)?;
                let table = txn.open_table(table).map_err(Into::<DBError>::into)?;
                tracer.measure();

                let mut tracer = Tracer::new_off(format!("KVStore({})::left_join", table.name()));
                while let Some(key_like) = rx_in.blocking_recv() {
                    let key_like: IK = key_like?;
                    let key: K = key_func(key_like.clone());
                    tracer.on();
                    let blob: Option<VO> = table
                        .get(key)
                        .map_err(Into::<DBError>::into)?
                        .map(|v| v.value().to_owned());
                    tracer.off();
                    tx.blocking_send(Ok((key_like, blob)))
                        .map_err(Into::<DBError>::into)?;
                }
                tracer.measure();

                let tracer =
                    Tracer::new_on(format!("KVStore({})::left_join::close", table.name()));
                drop(table);
                txn.close().map_err(Into::<DBError>::into)?;
                tracer.measure();

                Ok(())
            })();

            if let Err(e) = work {
                log::error!("failed to lookup({}) in redb: {:?}", table.name(), e);
                let _ = tx.blocking_send(Err(e));
            }
        });

        tokio::spawn(async move {
            let mut s = s.boxed();
            let mut count = 0u64;
            while let Some(item) = s.next().await {
                if let Err(e) = tx_in.send(item).await {
                    log::error!("failed to lookup({}) in redb: {}", table.name(), e);
                    return Err::<_, DBError>(e.into());
                }
                count += 1;
            }
            drop(tx_in);
            Ok(count)
        });

        rx.boxed()
    }
}

impl<K, V> KVStore<K, V, K, V>
where
    K: Key + Clone + for<'a> Borrow<<K as Value>::SelfType<'a>> + Send + Sync + 'static,
    V: Value + Clone + for<'a> Borrow<<V as Value>::SelfType<'a>> + Send + Sync + 'static,
    for<'a> K::SelfType<'a>: ToOwned<Owned = K>,
    for<'a> V::SelfType<'a>: ToOwned<Owned = V>,
    K: Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
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
        let table = self.table;
        let (tx_in, mut rx_in) = mpsc::channel::<Result<_, E>>(DEFAULT_BUFFER_SIZE);
        let (tx, rx) = flightdeck::tracked::mpsc_channel(
            format!("Redb::streaming_upsert({})", self.table.name()),
            DEFAULT_BUFFER_SIZE,
        );

        let bg = task::spawn_blocking(move || -> Result<u64, DBError> {
            let tracer = Tracer::new_on(format!(
                "KVStore({})::streaming_upsert::acq",
                table.name()
            ));
            let guard = db.read();
            tracer.measure();

            let tracer = Tracer::new_on(format!(
                "KVStore({})::streaming_upsert::open",
                table.name()
            ));
            let txn = guard
                .as_ref()
                .ok_or(DBError::AccessAfterDrop)?
                .begin_write()?;
            let mut table = txn.open_table(table)?;
            tracer.measure();

            let mut tracer =
                Tracer::new_off(format!("KVStore({})::streaming_upsert", table.name()));
            let mut count = 0u64;
            {
                while let Some(upsert) = rx_in.blocking_recv() {
                    let upsert: U = match upsert {
                        Ok(i) => i,
                        Err(e) => {
                            tx.blocking_send(Err(e))?;
                            continue;
                        }
                    };

                    let upsert_clone = upsert.clone();
                    let key = upsert.key();
                    tracer.on();
                    let (delete_key, uv) = match table.get_mut(upsert.key())? {
                        Some(mut guard) => match upsert.upsert(Some(guard.value())) {
                            UpsertAction::Change(v) => {
                                count += 1;
                                guard.insert(v.clone())?;
                                let uv = UpsertedValue::<U, V> {
                                    upsert: upsert_clone,
                                    previous_value: Some(guard.value().to_owned()),
                                };
                                (false, uv)
                            }
                            UpsertAction::Delete => {
                                let uv = UpsertedValue {
                                    upsert: upsert_clone,
                                    previous_value: Some(guard.value().to_owned()),
                                };
                                (true, uv)
                            }
                            UpsertAction::NoChange => {
                                let uv = UpsertedValue {
                                    upsert: upsert_clone,
                                    previous_value: Some(guard.value().to_owned()),
                                };
                                (false, uv)
                            }
                        },
                        None => {
                            let uv = if let UpsertAction::Change(v) = upsert.upsert(None) {
                                count += 1;
                                table.insert(key.clone(), v.clone())?;
                                UpsertedValue {
                                    upsert: upsert_clone,
                                    previous_value: None,
                                }
                            } else {
                                UpsertedValue {
                                    upsert: upsert_clone,
                                    previous_value: None,
                                }
                            };
                            (false, uv)
                        }
                    };
                    if delete_key {
                        count += 1;
                        table.remove(key.clone())?;
                    }
                    tracer.off();
                    tx.blocking_send(Ok(uv))?;
                }
            }
            tracer.measure();

            let tracer = Tracer::new_on(format!(
                "KVStore({})::streaming_upsert::commit",
                table.name()
            ));
            drop(table);
            if count > 0 {
                txn.commit()?;
            } else {
                txn.abort()?;
            }
            tracer.measure();

            Ok(count)
        });

        let table = self.table;
        let bg = tokio::spawn(async move {
            let mut s = s.boxed();
            let mut count = 0u64;
            while let Some(item) = s.next().await {
                if let Err(e) = tx_in.send(item).await {
                    log::error!(
                        "failed to streaming_upsert({}) in redb: {}",
                        table.name(),
                        e
                    );
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
