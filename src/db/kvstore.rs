use crate::db::error::DBError;
use crate::flightdeck;
use crate::flightdeck::tracer::Tracer;
use crate::flightdeck::tracked::stream::{Adapter, TrackedStream};
use futures::StreamExt;
use futures_core::Stream;
use parking_lot::RwLock;
use redb::{Database, Key, ReadableDatabase, TableDefinition, TableHandle, Value};
use std::borrow::Borrow;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task;

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

pub struct DatabaseGuard {
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
        let tracer = Tracer::new_on(format!("RedbTable({})::close", name));
        drop(db);
        tracer.measure();
    }
}
impl Drop for DatabaseGuard {
    fn drop(&mut self) {
        if let Some(_db) = self.db.take() {
            #[cfg(debug_assertions)]
            panic!("Use ::close instead of relying on the default Drop behaviour");

            #[cfg(not(debug_assertions))]
            {
                let tracer = Tracer::new_on(format!("RedbTable({})::drop", self.name));
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
    K: Key + Clone + Send + Sync + for<'a> Borrow<<K as Value>::SelfType<'a>> + 'static,
    V: Value + for<'a> Borrow<<V as Value>::SelfType<'a>> + Send + Sync + 'static,
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
        let tracer = Tracer::new_on(format!("RedbTable({})::compact", self.table.name()));
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let mut guard = db.write();
            Ok::<_, DBError>(guard.as_mut().ok_or(DBError::AccessAfterDrop)?.compact()?)
        })
        .await??;
        tracer.measure();
        Ok(())
    }

    pub(crate) fn stream(
        &self,
    ) -> TrackedStream<impl Stream<Item = Result<(KO, VO), DBError>> + 'static, Adapter> {
        let table = self.table;
        let db = self.db.clone();
        let name = format!("RedbTable({})::stream", table.name());
        let (tx, rx) = flightdeck::tracked::mpsc_channel(name, DEFAULT_BUFFER_SIZE);
        task::spawn_blocking(move || {
            let work: Result<(), DBError> = (|| {
                let tracer = Tracer::new_on(format!("RedbTable({})::stream::acq", table.name()));
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
                Ok(())
            })();

            if let Err(e) = work {
                log::error!("failed to stream {} from redb: {}", table.name(), e);
                let _ = tx.blocking_send(Err(e));
            }
        });

        rx
    }

    pub(crate) async fn apply(
        &self,
        s: impl Stream<Item = Result<(K, Option<V>), DBError>> + Send + 'static,
    ) -> Result<u64, DBError> {
        let (tx, mut rx) = mpsc::channel::<Result<_, DBError>>(DEFAULT_BUFFER_SIZE);
        let db = self.db.clone();
        let table = self.table;

        let bg = task::spawn_blocking(move || -> Result<u64, DBError> {
            let tracer = Tracer::new_on(format!("RedbTable({})::apply::acq", table.name()));
            let guard = db.read();
            tracer.measure();

            let tracer = Tracer::new_on(format!("RedbTable({})::apply::open", table.name()));
            let txn = guard
                .as_ref()
                .ok_or(DBError::AccessAfterDrop)?
                .begin_write()?;
            let mut table = txn.open_table(table)?;
            tracer.measure();

            let mut tracer = Tracer::new_off(format!("RedbTable({})::apply", table.name()));
            let mut count = 0u64;
            {
                while let Some(item) = rx.blocking_recv() {
                    count += 1;
                    let (key, value) = item?;
                    tracer.on();
                    match value {
                        None => table.remove(key)?,
                        Some(v) => table.insert(key, v)?,
                    };
                    tracer.off();
                }
            }

            tracer.on();
            drop(table);
            if count > 0 {
                txn.commit()?;
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

    pub(crate) async fn upsert<U: Upsert<K = K, V = V>>(
        &self,
        s: impl Stream<Item = Result<U, DBError>> + Send + 'static,
    ) -> Result<u64, DBError> {
        let (tx, mut rx) = mpsc::channel::<Result<_, DBError>>(DEFAULT_BUFFER_SIZE);
        let table = self.table;
        let db = self.db.clone();
        let bg = task::spawn_blocking(move || -> Result<u64, DBError> {
            let tracer = Tracer::new_on(format!("RedbTable({})::upsert::acq", table.name()));
            let guard = db.read();
            tracer.measure();

            let tracer = Tracer::new_on(format!("RedbTable({})::upsert::open", table.name()));
            let txn = guard
                .as_ref()
                .ok_or(DBError::AccessAfterDrop)?
                .begin_write()?;
            let mut table = txn.open_table(table)?;
            tracer.measure();

            let mut tracer = Tracer::new_off(format!("RedbTable({})::upsert", table.name()));
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

            tracer.on();
            drop(table);
            if count > 0 {
                txn.commit()?;
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
        s: impl Stream<Item = Result<IK, E>> + Send + 'static,
        key_func: KF,
    ) -> impl Stream<Item = Result<(IK, Option<VO>), E>> + Send + 'static
    where
        KF: Fn(IK) -> K + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        let table = self.table;
        let db = self.db.clone();
        let (tx_in, mut rx_in) = mpsc::channel::<Result<_, E>>(DEFAULT_BUFFER_SIZE);
        let (tx, rx) = flightdeck::tracked::mpsc_channel::<Result<_, E>>(
            format!("RedbTable({})::left_join", table.name()),
            DEFAULT_BUFFER_SIZE,
        );
        task::spawn_blocking(move || {
            let work: Result<(), E> = (|| {
                let tracer = Tracer::new_on(format!("RedbTable({})::left_join::acq", table.name()));
                let guard = db.read();
                tracer.measure();

                let tracer =
                    Tracer::new_on(format!("RedbTable({})::left_join::open", table.name()));
                let txn = guard
                    .as_ref()
                    .ok_or(DBError::AccessAfterDrop)?
                    .begin_read()
                    .map_err(Into::<DBError>::into)?;
                let table = txn.open_table(table).map_err(Into::<DBError>::into)?;
                tracer.measure();

                let mut tracer = Tracer::new_off(format!("RedbTable({})::left_join", table.name()));
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

        rx
    }
}
