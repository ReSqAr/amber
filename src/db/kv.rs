use crate::db::error::DBError;
use crate::db::models::{
    Blob, BlobRef, Connection, ConnectionMetadata, ConnectionName, CurrentBlob, CurrentCheck,
    CurrentFile, CurrentMaterialisation, CurrentObservation, CurrentRepository,
    CurrentRepositoryName, File, FileCheck, FileSeen, InsertMaterialisation, LogOffset, Path,
    RepoID, Repository, RepositoryMetadata, RepositoryName, TableName,
};
use crate::flightdeck;
use crate::flightdeck::tracer::Tracer;
use crate::flightdeck::tracked::stream::{Adapter, TrackedStream};
use futures::{StreamExt, TryStreamExt, stream};
use futures_core::Stream;
use parking_lot::RwLock;
use redb::{Database, Key, ReadableDatabase, TableDefinition, TableHandle, Value};
use std::borrow::Borrow;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::create_dir_all;
use tokio::sync::mpsc;
use tokio::task;

const DEFAULT_BUFFER_SIZE: usize = 100;

const CURRENT_BLOBS: TableDefinition<BlobRef, CurrentBlob> = TableDefinition::new("current_blobs");

const CURRENT_FILES: TableDefinition<Path, CurrentFile> = TableDefinition::new("current_files");

const CURRENT_MATERIALISATIONS: TableDefinition<Path, CurrentMaterialisation> =
    TableDefinition::new("current_materialisations");

const CURRENT_REPOSITORY_NAMES: TableDefinition<RepoID, CurrentRepositoryName> =
    TableDefinition::new("current_repository_names");

const CURRENT_OBSERVATIONS: TableDefinition<Path, CurrentObservation> =
    TableDefinition::new("current_observations");

const CURRENT_CHECKS: TableDefinition<Path, CurrentCheck> = TableDefinition::new("current_checks");

const CURRENT_REPOSITORY: TableDefinition<(), CurrentRepository> =
    TableDefinition::new("current_repository");

const REPOSITORIES: TableDefinition<RepoID, RepositoryMetadata> =
    TableDefinition::new("repositories");

const CONNECTIONS: TableDefinition<ConnectionName, ConnectionMetadata> =
    TableDefinition::new("connections");

const CURRENT_REDUCTIONS: TableDefinition<TableName, LogOffset> =
    TableDefinition::new("current_reductions");

enum UpsertAction<T> {
    NoChange,
    Change(T),
    Delete,
}

trait Upsert: Sync + Send + 'static {
    type K: Key + for<'a> Borrow<<<Self as Upsert>::K as Value>::SelfType<'a>> + Send + Sync + Clone;
    type V: Value + for<'a> Borrow<<<Self as Upsert>::V as Value>::SelfType<'a>> + Send + Sync;

    fn key(&self) -> Self::K;
    fn upsert(self, v: Option<<Self::V as Value>::SelfType<'_>>) -> UpsertAction<Self::V>;
}

struct UpsertBlob(Blob);
impl Upsert for UpsertBlob {
    type K = BlobRef;
    type V = CurrentBlob;

    fn key(&self) -> BlobRef {
        BlobRef {
            blob_id: self.0.blob_id.clone(),
            repo_id: self.0.repo_id.clone(),
        }
    }

    fn upsert(self, v: Option<CurrentBlob>) -> UpsertAction<CurrentBlob> {
        if let Some(v) = v
            && v.valid_from > self.0.valid_from
        {
            return UpsertAction::NoChange;
        }

        match self.0.has_blob {
            true => UpsertAction::Change(CurrentBlob {
                blob_path: self.0.path,
                valid_from: self.0.valid_from,
                blob_size: self.0.blob_size,
            }),
            false => UpsertAction::Delete,
        }
    }
}

struct UpsertFile(File);
impl Upsert for UpsertFile {
    type K = Path;
    type V = CurrentFile;

    fn key(&self) -> Path {
        self.0.path.clone()
    }

    fn upsert(self, v: Option<CurrentFile>) -> UpsertAction<CurrentFile> {
        if let Some(v) = v
            && v.valid_from > self.0.valid_from
        {
            return UpsertAction::NoChange;
        }

        match self.0.blob_id {
            Some(blob_id) => UpsertAction::Change(CurrentFile {
                blob_id,
                valid_from: self.0.valid_from,
            }),
            None => UpsertAction::Delete,
        }
    }
}
struct UpsertMaterialisation(InsertMaterialisation);
impl Upsert for UpsertMaterialisation {
    type K = Path;
    type V = CurrentMaterialisation;

    fn key(&self) -> Path {
        self.0.path.clone()
    }

    fn upsert(self, v: Option<CurrentMaterialisation>) -> UpsertAction<CurrentMaterialisation> {
        if let Some(v) = v
            && v.valid_from > self.0.valid_from
        {
            return UpsertAction::NoChange;
        }

        match self.0.blob_id {
            Some(blob_id) => UpsertAction::Change(CurrentMaterialisation {
                blob_id,
                valid_from: self.0.valid_from,
            }),
            None => UpsertAction::Delete,
        }
    }
}

struct UpsertRepositoryName(RepositoryName);
impl Upsert for UpsertRepositoryName {
    type K = RepoID;
    type V = CurrentRepositoryName;

    fn key(&self) -> RepoID {
        self.0.repo_id.clone()
    }

    fn upsert(self, v: Option<CurrentRepositoryName>) -> UpsertAction<CurrentRepositoryName> {
        if let Some(v) = v
            && v.valid_from > self.0.valid_from
        {
            return UpsertAction::NoChange;
        }

        UpsertAction::Change(CurrentRepositoryName {
            name: self.0.name,
            valid_from: self.0.valid_from,
        })
    }
}

struct UpsertRepositoryMetadata(Repository);
impl Upsert for UpsertRepositoryMetadata {
    type K = RepoID;
    type V = RepositoryMetadata;

    fn key(&self) -> RepoID {
        self.0.repo_id.clone()
    }

    fn upsert(self, v: Option<RepositoryMetadata>) -> UpsertAction<RepositoryMetadata> {
        let merge = |l, r| match (l, r) {
            (Some(l), Some(r)) => Some(std::cmp::max(l, r)),
            (Some(l), None) => Some(l),
            (None, Some(r)) => Some(r),
            (None, None) => None,
        };
        match v {
            Some(v) => UpsertAction::Change(RepositoryMetadata {
                last_file_index: merge(v.last_file_index, self.0.last_file_index),
                last_blob_index: merge(v.last_blob_index, self.0.last_blob_index),
                last_name_index: merge(v.last_name_index, self.0.last_name_index),
            }),
            None => UpsertAction::Change(RepositoryMetadata {
                last_file_index: self.0.last_file_index,
                last_blob_index: self.0.last_blob_index,
                last_name_index: self.0.last_name_index,
            }),
        }
    }
}

#[derive(Clone)]
pub(crate) struct KVStores {
    current_blobs: KVStore<BlobRef, CurrentBlob, BlobRef, CurrentBlob>,
    current_files: KVStore<Path, CurrentFile, Path, CurrentFile>,
    current_materialisations: KVStore<Path, CurrentMaterialisation, Path, CurrentMaterialisation>,
    current_repository_names: KVStore<RepoID, CurrentRepositoryName, RepoID, CurrentRepositoryName>,
    current_observations: KVStore<Path, CurrentObservation, Path, CurrentObservation>,
    current_checks: KVStore<Path, CurrentCheck, Path, CurrentCheck>,
    current_repository: KVStore<(), CurrentRepository, (), CurrentRepository>,
    repositories: KVStore<RepoID, RepositoryMetadata, RepoID, RepositoryMetadata>,
    connections: KVStore<ConnectionName, ConnectionMetadata, ConnectionName, ConnectionMetadata>,
    current_reductions: KVStore<TableName, LogOffset, TableName, LogOffset>,
}

impl KVStores {
    pub(crate) async fn new(base_path: PathBuf) -> Result<Self, DBError> {
        create_dir_all(&base_path).await?;

        let tracer = Tracer::new_on("KVStores::new");

        let current_blobs = KVStore::new(base_path.join("current_blobs.redb"), CURRENT_BLOBS);
        let current_files = KVStore::new(base_path.join("current_files.redb"), CURRENT_FILES);
        let current_materialisations = KVStore::new(
            base_path.join("current_materialisations.redb"),
            CURRENT_MATERIALISATIONS,
        );
        let current_repository_names = KVStore::new(
            base_path.join("current_repository_names.redb"),
            CURRENT_REPOSITORY_NAMES,
        );
        let current_observations = KVStore::new(
            base_path.join("current_observations.redb"),
            CURRENT_OBSERVATIONS,
        );
        let current_checks = KVStore::new(base_path.join("current_checks.redb"), CURRENT_CHECKS);
        let current_repository = KVStore::new(
            base_path.join("current_repository.redb"),
            CURRENT_REPOSITORY,
        );
        let repositories = KVStore::new(base_path.join("repositories.redb"), REPOSITORIES);
        let connections = KVStore::new(base_path.join("connections.redb"), CONNECTIONS);
        let current_reductions = KVStore::new(
            base_path.join("current_reductions.redb"),
            CURRENT_REDUCTIONS,
        );

        let (
            current_blobs,
            current_files,
            current_materialisations,
            current_repository_names,
            current_observations,
            current_checks,
            current_repository,
            repositories,
            connections,
            current_reductions,
        ) = tokio::try_join!(
            current_blobs,
            current_files,
            current_materialisations,
            current_repository_names,
            current_observations,
            current_checks,
            current_repository,
            repositories,
            connections,
            current_reductions,
        )?;
        tracer.measure();

        Ok(Self {
            current_blobs,
            current_files,
            current_materialisations,
            current_repository_names,
            current_observations,
            current_checks,
            current_repository,
            repositories,
            connections,
            current_reductions,
        })
    }

    #[allow(dead_code)]
    async fn compact(&self) -> Result<(), DBError> {
        tokio::try_join!(
            self.current_blobs.compact(),
            self.current_files.compact(),
            self.current_materialisations.compact(),
            self.current_observations.compact(),
            self.current_checks.compact(),
        )?;

        Ok(())
    }

    pub(crate) async fn apply_blobs(
        &self,
        s: impl Stream<Item = Result<Blob, DBError>> + Send + 'static,
    ) -> Result<u64, DBError> {
        let s = s.map(move |r| r.map(UpsertBlob));
        self.current_blobs.upsert(s).await
    }

    pub(crate) async fn apply_files(
        &self,
        s: impl Stream<Item = Result<File, DBError>> + Send + 'static,
    ) -> Result<u64, DBError> {
        let s = s.map(move |r| r.map(UpsertFile));
        self.current_files.upsert(s).await
    }

    pub(crate) async fn apply_materialisations(
        &self,
        s: impl Stream<Item = Result<InsertMaterialisation, DBError>> + Send + 'static,
    ) -> Result<u64, DBError> {
        let s = s.map(move |r| r.map(UpsertMaterialisation));
        self.current_materialisations.upsert(s).await
    }

    pub(crate) async fn apply_repository_names(
        &self,
        s: impl Stream<Item = Result<RepositoryName, DBError>> + Send + 'static,
    ) -> Result<u64, DBError> {
        let s = s.map(move |r| r.map(UpsertRepositoryName));
        self.current_repository_names.upsert(s).await
    }

    pub(crate) async fn apply_file_seen(
        &self,
        s: impl Stream<Item = Result<FileSeen, DBError>> + Send + 'static,
    ) -> Result<u64, DBError> {
        let map = |e: FileSeen| {
            (
                e.path,
                Some(CurrentObservation {
                    fs_last_seen_id: e.seen_id,
                    fs_last_seen_dttm: e.seen_dttm,
                    fs_last_modified_dttm: e.last_modified_dttm,
                    fs_last_size: e.size,
                }),
            )
        };
        let s = s.map(move |r| r.map(map));
        self.current_observations.apply(s).await
    }

    pub(crate) async fn apply_file_checks(
        &self,
        s: impl Stream<Item = Result<FileCheck, DBError>> + Send + 'static,
    ) -> Result<u64, DBError> {
        let map = |e: FileCheck| {
            (
                e.path,
                Some(CurrentCheck {
                    check_last_dttm: e.check_dttm,
                    check_last_hash: e.hash,
                }),
            )
        };
        let s = s.map(move |r| r.map(map));
        self.current_checks.apply(s).await
    }

    pub(crate) async fn store_current_repository(
        &self,
        current_repository: CurrentRepository,
    ) -> Result<u64, DBError> {
        self.current_repository
            .apply(stream::iter([Ok(((), Some(current_repository)))]))
            .await
    }

    pub(crate) async fn apply_current_reductions(
        &self,
        s: impl Stream<Item = (TableName, LogOffset)> + Send + 'static,
    ) -> Result<u64, DBError> {
        let s = s.map(|(t, o)| Ok((t, Some(o))));
        self.current_reductions.apply(s).await
    }

    pub(crate) async fn store_connection(
        &self,
        Connection {
            name,
            connection_type,
            parameter,
        }: Connection,
    ) -> Result<u64, DBError> {
        self.connections
            .apply(stream::iter([Ok((
                name,
                Some(ConnectionMetadata {
                    connection_type,
                    parameter,
                }),
            ))]))
            .await
    }

    pub(crate) async fn apply_repositories(
        &self,
        s: impl Stream<Item = Result<Repository, DBError>> + Send + 'static,
    ) -> Result<u64, DBError> {
        let s = s.map(move |r| r.map(UpsertRepositoryMetadata));
        self.repositories.upsert(s).await
    }

    pub(crate) fn stream_current_blobs(
        &self,
    ) -> impl Stream<Item = Result<(BlobRef, CurrentBlob), DBError>> + Send + 'static {
        self.current_blobs.stream()
    }

    pub(crate) fn stream_current_files(
        &self,
    ) -> impl Stream<Item = Result<(Path, CurrentFile), DBError>> + Send + 'static {
        self.current_files.stream()
    }

    pub(crate) fn stream_current_repository_names(
        &self,
    ) -> impl Stream<Item = Result<(RepoID, CurrentRepositoryName), DBError>> + Send + 'static {
        self.current_repository_names.stream()
    }

    pub(crate) async fn load_current_repository(
        &self,
    ) -> Result<Option<CurrentRepository>, DBError> {
        let v: Vec<_> = self.current_repository.stream().try_collect().await?;
        Ok(v.into_iter().next().map(|(_, cr)| cr))
    }

    pub(crate) fn stream_repositories(
        &self,
    ) -> impl Stream<Item = Result<(RepoID, RepositoryMetadata), DBError>> + Send + 'static {
        self.repositories.stream()
    }

    pub(crate) fn stream_connections(
        &self,
    ) -> impl Stream<Item = Result<(ConnectionName, ConnectionMetadata), DBError>> + Send + 'static
    {
        self.connections.stream()
    }

    pub(crate) fn stream_current_reductions(
        &self,
    ) -> impl Stream<Item = Result<(TableName, LogOffset), DBError>> + Send + 'static {
        self.current_reductions.stream()
    }

    pub(crate) fn left_join_current_blobs<IK, KF>(
        &self,
        s: impl Stream<Item = Result<IK, DBError>> + Send + 'static,
        key_func: KF,
    ) -> impl Stream<Item = Result<(IK, Option<CurrentBlob>), DBError>> + Send + 'static
    where
        KF: Fn(IK) -> BlobRef + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.current_blobs.left_join(s, key_func)
    }

    pub(crate) fn left_join_current_files<
        IK,
        KF,
        E: From<DBError> + Debug + Send + Sync + 'static,
    >(
        &self,
        s: impl Stream<Item = Result<IK, E>> + Send + 'static,
        key_func: KF,
    ) -> impl Stream<Item = Result<(IK, Option<CurrentFile>), E>> + Send + 'static
    where
        KF: Fn(IK) -> Path + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.current_files.left_join(s, key_func)
    }

    pub(crate) fn left_join_current_materialisations<
        IK,
        KF,
        E: From<DBError> + Debug + Send + Sync + 'static,
    >(
        &self,
        s: impl Stream<Item = Result<IK, E>> + Send + 'static,
        key_func: KF,
    ) -> impl Stream<Item = Result<(IK, Option<CurrentMaterialisation>), E>> + Send + 'static
    where
        KF: Fn(IK) -> Path + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.current_materialisations.left_join(s, key_func)
    }

    pub(crate) fn left_join_current_repository_names<
        IK,
        KF,
        E: From<DBError> + Debug + Send + Sync + 'static,
    >(
        &self,
        s: impl Stream<Item = Result<IK, E>> + Send + 'static,
        key_func: KF,
    ) -> impl Stream<Item = Result<(IK, Option<CurrentRepositoryName>), E>> + Send + 'static
    where
        KF: Fn(IK) -> RepoID + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.current_repository_names.left_join(s, key_func)
    }

    pub(crate) fn left_join_current_observations<
        IK,
        KF,
        E: From<DBError> + Debug + Send + Sync + 'static,
    >(
        &self,
        s: impl Stream<Item = Result<IK, E>> + Send + 'static,
        key_func: KF,
    ) -> impl Stream<Item = Result<(IK, Option<CurrentObservation>), E>> + Send + 'static
    where
        KF: Fn(IK) -> Path + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.current_observations.left_join(s, key_func)
    }

    pub(crate) fn left_join_current_check<
        IK,
        KF,
        E: From<DBError> + Debug + Send + Sync + 'static,
    >(
        &self,
        s: impl Stream<Item = Result<IK, E>> + Send + 'static,
        key_func: KF,
    ) -> impl Stream<Item = Result<(IK, Option<CurrentCheck>), E>> + Send + 'static
    where
        KF: Fn(IK) -> Path + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.current_checks.left_join(s, key_func)
    }

    pub(crate) fn left_join_repositories<IK, KF, E: From<DBError> + Debug + Send + Sync + 'static>(
        &self,
        s: impl Stream<Item = Result<IK, E>> + Send + 'static,
        key_func: KF,
    ) -> impl Stream<Item = Result<(IK, Option<RepositoryMetadata>), E>> + Send + 'static
    where
        KF: Fn(IK) -> RepoID + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.repositories.left_join(s, key_func)
    }

    pub(crate) fn left_join_connections<IK, KF, E: From<DBError> + Debug + Send + Sync + 'static>(
        &self,
        s: impl Stream<Item = Result<IK, E>> + Send + 'static,
        key_func: KF,
    ) -> impl Stream<Item = Result<(IK, Option<ConnectionMetadata>), E>> + Send + 'static
    where
        KF: Fn(IK) -> ConnectionName + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.connections.left_join(s, key_func)
    }
}

pub(crate) struct KVStore<K, V, KO, VO>
where
    K: Key + 'static,
    V: Value + 'static,
    for<'a> K::SelfType<'a>: ToOwned<Owned = KO>,
    for<'a> V::SelfType<'a>: ToOwned<Owned = VO>,
{
    db: Arc<RwLock<Database>>,
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
                Database::create(path)?
            };

            let db = Arc::new(RwLock::new(db));
            {
                let txn = db.read().begin_write()?;
                {
                    let _ = txn.open_table(table)?;
                }
                txn.commit()?;
            }

            Ok(Self { db, table })
        })
        .await?
    }

    async fn compact(&self) -> Result<(), DBError> {
        let tracer = Tracer::new_on(format!("RedbTable({})::compact", self.table.name()));
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let mut guard = db.write();
            guard.compact()
        })
        .await??;
        tracer.measure();
        Ok(())
    }

    fn stream(
        &self,
    ) -> TrackedStream<impl Stream<Item = Result<(KO, VO), DBError>> + 'static, Adapter> {
        let table = self.table;
        let db = self.db.clone();
        let name = format!("RedbTable({})::stream", table.name());
        let (tx, rx) = flightdeck::tracked::mpsc_channel(name, DEFAULT_BUFFER_SIZE);
        task::spawn_blocking(move || {
            let work: Result<(), DBError> = (|| {
                let tracer = Tracer::new_on(format!("RedbTable({})::stream::acq", table.name()));
                let txn = db.read().begin_read()?;
                tracer.measure();

                let table = txn.open_table(table)?;
                for item in table.range::<K>(..)? {
                    let (key, value) = item?;
                    let key_owned: KO = key.value().to_owned();
                    let value_owned: VO = value.value().to_owned();
                    tx.blocking_send(Ok((key_owned, value_owned)))
                        .map_err(|e| DBError::SendError(format!("{:?}", e)))?;
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

    async fn apply(
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
            let txn = guard.begin_write()?;
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

    async fn upsert<U: Upsert<K = K, V = V>>(
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
            let txn = guard.begin_write()?;
            let mut table = txn.open_table(table)?;
            tracer.measure();

            let mut tracer = Tracer::new_off(format!("RedbTable({})::upsert", table.name()));
            let mut count = 0u64;
            {
                while let Some(item) = rx.blocking_recv() {
                    count += 1;
                    let item: U = item?;
                    let key = item.key();
                    tracer.on();
                    let delete_key = match table.get_mut(item.key())? {
                        Some(mut guard) => match item.upsert(Some(guard.value())) {
                            UpsertAction::Change(v) => {
                                guard.insert(v)?;
                                false
                            }
                            UpsertAction::Delete => true,
                            UpsertAction::NoChange => false,
                        },
                        None => {
                            if let UpsertAction::Change(v) = item.upsert(None) {
                                table.insert(key.clone(), v)?;
                            }
                            false
                        }
                    };
                    if delete_key {
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

    fn left_join<IK, KF, E: From<DBError> + Debug + Send + Sync + 'static>(
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
                let txn = guard.begin_read().map_err(Into::<DBError>::into)?;
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
                        .map_err(|e| DBError::SendError(format!("{:?}", e)))?;
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
                    return Err(DBError::SendError(format!("{:?}", e)));
                }
                count += 1;
            }
            drop(tx_in);
            Ok(count)
        });

        rx
    }
}
