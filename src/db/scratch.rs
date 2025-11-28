use crate::db::error::DBError;
use crate::db::models::Path;
use crate::flightdeck;
use crate::utils::errors::InternalError;
use futures::StreamExt;
use futures_core::stream::BoxStream;
use parking_lot::RwLock;
use redb::{Database, ReadableDatabase, TableDefinition, TableHandle};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task;
use tokio::task::JoinHandle;

const DEFAULT_BATCH_SIZE: usize = 100;

pub(crate) struct ScratchKVStore<V, VO = V>
where
    V: redb::Value + Send + Sync + 'static,
    for<'a> V::SelfType<'a>: ToOwned<Owned = VO>,
    VO: Send + Sync + Clone + 'static,
{
    db: Arc<RwLock<Database>>,
    table: TableDefinition<'static, Path, V>,
    _phantom_data_v: PhantomData<V>,
    _phantom_data_vo: PhantomData<VO>,
}

impl<V, VO> Clone for ScratchKVStore<V, VO>
where
    V: redb::Value + Send + Sync + 'static,
    for<'a> V::SelfType<'a>: ToOwned<Owned = VO>,
    VO: Send + Sync + Clone + 'static,
{
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            table: self.table,
            _phantom_data_v: Default::default(),
            _phantom_data_vo: Default::default(),
        }
    }
}

pub(crate) struct UpsertedValue<IK, V, VO> {
    pub(crate) path_like: IK,
    pub(crate) current_value: V,
    pub(crate) previous_value: Option<VO>,
}

impl<V, VO> ScratchKVStore<V, VO>
where
    V: redb::Value
        + Send
        + Sync
        + Clone
        + 'static
        + for<'a> std::borrow::Borrow<<V as redb::Value>::SelfType<'a>>,
    for<'a> V::SelfType<'a>: ToOwned<Owned = VO>,
    VO: Send + Sync + Clone + 'static,
{
    pub(crate) fn new(path: &std::path::Path) -> Result<Self, InternalError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = if path.exists() {
            Database::open(path).map_err(DBError::from)?
        } else {
            Database::create(path).map_err(DBError::from)?
        };

        let db = Arc::new(RwLock::new(db));
        let table = TableDefinition::new("scratch");

        {
            let txn = db.read().begin_write().map_err(DBError::from)?;
            {
                let _ = txn.open_table(table).map_err(DBError::from)?;
            }
            txn.commit().map_err(DBError::from)?;
        }

        Ok(Self {
            db,
            table,
            _phantom_data_v: Default::default(),
            _phantom_data_vo: Default::default(),
        })
    }

    pub(crate) async fn insert(
        &self,
        s: BoxStream<'static, Result<(Path, V), InternalError>>,
    ) -> Result<u64, InternalError> {
        let (tx, mut rx) = mpsc::channel::<Result<_, InternalError>>(DEFAULT_BATCH_SIZE);
        let db = self.db.clone();
        let table = self.table;
        let bg = task::spawn_blocking(move || -> Result<u64, InternalError> {
            let txn = db.read().begin_write().map_err(DBError::from)?;
            let mut count = 0u64;
            {
                let mut table = txn.open_table(table).map_err(DBError::from)?;
                while let Some(item) = rx.blocking_recv() {
                    let (key, value) = item?;
                    table.insert(key, value).map_err(DBError::from)?;
                    count += 1;
                }
            }

            txn.commit().map_err(DBError::from)?;
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

    pub(crate) fn left_join<IK, KF>(
        &self,
        s: BoxStream<'static, Result<IK, InternalError>>,
        key_func: KF,
    ) -> (
        BoxStream<'static, Result<(IK, Option<VO>), InternalError>>,
        JoinHandle<Result<u64, InternalError>>,
    )
    where
        KF: Fn(IK) -> Path + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        let db = self.db.clone();
        let (tx_in, mut rx_in) = mpsc::channel::<Result<_, InternalError>>(DEFAULT_BATCH_SIZE);
        let (tx, rx) = flightdeck::tracked::mpsc_channel(
            format!("Redb::lookup({})", self.table.name()),
            DEFAULT_BATCH_SIZE,
        );
        let table = self.table;
        task::spawn_blocking(move || {
            let work: Result<(), InternalError> = (|| {
                let txn = db.read().begin_read().map_err(DBError::from)?;
                let table = txn.open_table(table).map_err(DBError::from)?;

                while let Some(key_like) = rx_in.blocking_recv() {
                    let key_like: IK = key_like?;
                    let key: Path = key_func(key_like.clone());
                    let value = table
                        .get(&key)
                        .map_err(DBError::from)?
                        .map(|v| v.value().to_owned());
                    tx.blocking_send(Ok((key_like, value)))?;
                }

                Ok(())
            })();

            if let Err(e) = work {
                log::error!("failed to lookup({}) in redb: {}", table.name(), e);
                let _ = tx.blocking_send(Err(e));
            }
        });

        let table = self.table;
        let bg = tokio::spawn(async move {
            let mut s = s.boxed();
            let mut count = 0u64;
            while let Some(item) = s.next().await {
                if let Err(e) = tx_in.send(item).await {
                    log::error!("failed to lookup({}) in redb: {}", table.name(), e);
                    return Err(InternalError::DBError(e.into()));
                }
                count += 1;
            }
            drop(tx_in);
            Ok(count)
        });

        (rx.boxed(), bg)
    }

    pub(crate) fn stream(&self) -> BoxStream<'static, Result<(Path, VO), InternalError>> {
        let table = self.table.clone();
        let db = self.db.clone();
        let name = format!("RedbTable({})::stream", table.name());
        let (tx, rx) = flightdeck::tracked::mpsc_channel(name, DEFAULT_BATCH_SIZE);
        task::spawn_blocking(move || {
            let work: Result<(), DBError> = (|| {
                let txn = db.read().begin_read()?;
                let table = txn.open_table(table)?;
                for item in table.range::<Path>(..)? {
                    let (key, value) = item?;
                    let key_owned: Path = key.value().to_owned();
                    let value_owned: VO = value.value().to_owned();
                    tx.blocking_send(Ok((key_owned, value_owned)))?;
                }
                Ok(())
            })();

            if let Err(e) = work {
                log::error!("failed to stream {} from redb: {}", table.name(), e);
                let _ = tx.blocking_send(Err(e.into()));
            }
        });

        rx.boxed()
    }

    pub(crate) fn streaming_upsert<IK, KF>(
        &self,
        s: BoxStream<'static, Result<(IK, V), InternalError>>,
        key_func: KF,
    ) -> (
        BoxStream<'static, Result<UpsertedValue<IK, V, VO>, InternalError>>,
        JoinHandle<Result<u64, InternalError>>,
    )
    where
        KF: Fn(IK) -> Path + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        let db = self.db.clone();
        let (tx_in, mut rx_in) = mpsc::channel::<Result<_, InternalError>>(DEFAULT_BATCH_SIZE);
        let (tx, rx) = flightdeck::tracked::mpsc_channel(
            format!("Redb::lookup({})", self.table.name()),
            DEFAULT_BATCH_SIZE,
        );
        let table = self.table;
        task::spawn_blocking(move || {
            let work: Result<(), InternalError> = (|| {
                let txn = db.read().begin_write().map_err(DBError::from)?;
                {
                    let mut table = txn.open_table(table).map_err(DBError::from)?;
                    while let Some(item) = rx_in.blocking_recv() {
                        let (key_like, value): (IK, V) = item?;
                        let key: Path = key_func(key_like.clone());
                        let previous_value =
                            table.insert(key, value.clone()).map_err(DBError::from)?;
                        tx.blocking_send(Ok(UpsertedValue {
                            path_like: key_like,
                            current_value: value,
                            previous_value: previous_value.map(|v| v.value().to_owned()),
                        }))?;
                    }
                }

                txn.commit().map_err(DBError::from)?;
                Ok(())
            })();

            if let Err(e) = work {
                log::error!("failed to lookup({}) in redb: {}", table.name(), e);
                let _ = tx.blocking_send(Err(e));
            }
        });

        let table = self.table;
        let bg = tokio::spawn(async move {
            let mut s = s.boxed();
            let mut count = 0u64;
            while let Some(item) = s.next().await {
                if let Err(e) = tx_in.send(item).await {
                    log::error!("failed to lookup({}) in redb: {}", table.name(), e);
                    return Err(InternalError::DBError(e.into()));
                }
                count += 1;
            }
            drop(tx_in);
            Ok(count)
        });

        (rx.boxed(), bg)
    }
}
