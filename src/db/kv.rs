use crate::db::error::DBError;
use crate::db::kvstore::{KVStore, Upsert, UpsertAction};
use crate::db::models::{
    Blob, BlobRef, Connection, ConnectionMetadata, ConnectionName, CurrentBlob, CurrentCheck,
    CurrentFile, CurrentMaterialisation, CurrentObservation, CurrentRepository,
    CurrentRepositoryName, File, FileCheck, FileSeen, InsertMaterialisation, LogOffset, Path,
    RepoID, Repository, RepositoryMetadata, RepositoryName, TableName, Uid,
};
use crate::flightdeck::tracer::Tracer;
use futures::{StreamExt, TryStreamExt, stream};
use futures_core::Stream;
use redb::TableDefinition;
use std::fmt::Debug;
use std::path::PathBuf;
use tokio::fs::create_dir_all;

const KNOWN_BLOB_UIDS: TableDefinition<Uid, ()> = TableDefinition::new("known_blob_uids");
const KNOWN_FILE_UIDS: TableDefinition<Uid, ()> = TableDefinition::new("known_file_uids");
const KNOWN_REPOSITORY_NAME_UIDS: TableDefinition<Uid, ()> =
    TableDefinition::new("known_repository_name_uids");

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

struct UpsertCurrentReduction(TableName, LogOffset);
impl Upsert for UpsertCurrentReduction {
    type K = TableName;
    type V = LogOffset;

    fn key(&self) -> TableName {
        self.0.clone()
    }

    fn upsert(self, v: Option<LogOffset>) -> UpsertAction<LogOffset> {
        match v {
            None => UpsertAction::Change(self.1),
            Some(o) => {
                if self.1 > o {
                    UpsertAction::Change(self.1)
                } else {
                    UpsertAction::NoChange
                }
            }
        }
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
    known_blob_uids: KVStore<Uid, (), Uid, ()>,
    known_file_uids: KVStore<Uid, (), Uid, ()>,
    known_repository_name_uids: KVStore<Uid, (), Uid, ()>,
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

        let known_blob_uids = KVStore::new(base_path.join("known_blob_uids.redb"), KNOWN_BLOB_UIDS);
        let known_file_uids = KVStore::new(base_path.join("known_file_uids.redb"), KNOWN_FILE_UIDS);
        let known_repository_name_uids = KVStore::new(
            base_path.join("known_repository_name_uids.redb"),
            KNOWN_REPOSITORY_NAME_UIDS,
        );
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
            known_blob_uids,
            known_file_uids,
            known_repository_name_uids,
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
            known_blob_uids,
            known_file_uids,
            known_repository_name_uids,
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
            known_blob_uids,
            known_file_uids,
            known_repository_name_uids,
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

    pub(crate) async fn close(&self) -> Result<(), DBError> {
        tokio::try_join!(
            self.known_blob_uids.close(),
            self.known_file_uids.close(),
            self.known_repository_name_uids.close(),
            self.current_blobs.close(),
            self.current_files.close(),
            self.current_materialisations.close(),
            self.current_repository_names.close(),
            self.current_observations.close(),
            self.current_checks.close(),
            self.current_repository.close(),
            self.repositories.close(),
            self.connections.close(),
            self.current_reductions.close(),
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    async fn compact(&self) -> Result<(), DBError> {
        tokio::try_join!(
            self.known_blob_uids.compact(),
            self.known_file_uids.compact(),
            self.known_repository_name_uids.compact(),
            self.current_blobs.compact(),
            self.current_files.compact(),
            self.current_materialisations.compact(),
            self.current_repository_names.compact(),
            self.current_observations.compact(),
            self.current_checks.compact(),
            self.current_repository.compact(),
            self.repositories.compact(),
            self.connections.compact(),
            self.current_reductions.compact(),
        )?;
        Ok(())
    }

    pub(crate) async fn apply_known_blob_uids(
        &self,
        s: impl Stream<Item = Result<Uid, DBError>> + Send + 'static,
    ) -> Result<u64, DBError> {
        let s = s.map(|e| e.map(|u| (u, Some(()))));
        self.known_blob_uids.apply(s).await
    }

    pub(crate) async fn apply_known_file_uids(
        &self,
        s: impl Stream<Item = Result<Uid, DBError>> + Send + 'static,
    ) -> Result<u64, DBError> {
        let s = s.map(|e| e.map(|u| (u, Some(()))));
        self.known_file_uids.apply(s).await
    }

    pub(crate) async fn apply_known_repository_name_uids(
        &self,
        s: impl Stream<Item = Result<Uid, DBError>> + Send + 'static,
    ) -> Result<u64, DBError> {
        let s = s.map(|e| e.map(|u| (u, Some(()))));
        self.known_repository_name_uids.apply(s).await
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
        let s = s.map(|(t, o)| Ok(UpsertCurrentReduction(t, o)));
        self.current_reductions.upsert(s).await
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

    pub(crate) fn left_join_known_blob_uids<IK, KF>(
        &self,
        s: impl Stream<Item = Result<IK, DBError>> + Send + 'static,
        key_func: KF,
    ) -> impl Stream<Item = Result<(IK, Option<()>), DBError>> + Send + 'static
    where
        KF: Fn(IK) -> Uid + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.known_blob_uids.left_join(s, key_func)
    }

    pub(crate) fn left_join_known_file_uids<IK, KF>(
        &self,
        s: impl Stream<Item = Result<IK, DBError>> + Send + 'static,
        key_func: KF,
    ) -> impl Stream<Item = Result<(IK, Option<()>), DBError>> + Send + 'static
    where
        KF: Fn(IK) -> Uid + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.known_file_uids.left_join(s, key_func)
    }

    pub(crate) fn left_join_known_repository_name_uids<IK, KF>(
        &self,
        s: impl Stream<Item = Result<IK, DBError>> + Send + 'static,
        key_func: KF,
    ) -> impl Stream<Item = Result<(IK, Option<()>), DBError>> + Send + 'static
    where
        KF: Fn(IK) -> Uid + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.known_repository_name_uids.left_join(s, key_func)
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
