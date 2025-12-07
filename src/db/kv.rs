use crate::db::error::DBError;
use crate::db::kvstore::{KVStore, Upsert, UpsertAction};
use crate::db::models::{
    Blob, BlobRef, Connection, ConnectionMetadata, ConnectionName, CurrentBlob, CurrentCheck,
    CurrentFile, CurrentMaterialisation, CurrentObservation, CurrentRepository, File, FileCheck, FileSeen, InsertMaterialisation, LogOffset, Path,
    RepoID, Repository, RepositoryMetadata, TableName, Uid,
};
use crate::flightdeck::tracer::Tracer;
use futures::{StreamExt, TryStreamExt, stream};
use futures_core::stream::BoxStream;
use std::fmt::Debug;
use std::path::PathBuf;
use tokio::fs::create_dir_all;

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
    known_blob_uids: KVStore<Uid, ()>,
    known_file_uids: KVStore<Uid, ()>,
    current_blobs: KVStore<BlobRef, CurrentBlob>,
    current_files: KVStore<Path, CurrentFile>,
    current_materialisations: KVStore<Path, CurrentMaterialisation>,
    current_observations: KVStore<Path, CurrentObservation>,
    current_checks: KVStore<Path, CurrentCheck>,
    current_repository: KVStore<(), CurrentRepository>,
    repositories: KVStore<RepoID, RepositoryMetadata>,
    connections: KVStore<ConnectionName, ConnectionMetadata>,
    current_reductions: KVStore<TableName, LogOffset>,
}

impl KVStores {
    pub(crate) async fn new(base_path: PathBuf) -> Result<Self, DBError> {
        create_dir_all(&base_path).await?;

        let tracer = Tracer::new_on("KVStores::new");

        let known_blob_uids = KVStore::new(
            base_path.join("known_blob_uids.kv"),
            "known_blob_uids".to_string(),
        );
        let known_file_uids = KVStore::new(
            base_path.join("known_file_uids.kv"),
            "known_file_uids".to_string(),
        );
        let current_blobs = KVStore::new(
            base_path.join("current_blobs.kv"),
            "current_blobs".to_string(),
        );
        let current_files = KVStore::new(
            base_path.join("current_files.kv"),
            "current_files".to_string(),
        );
        let current_materialisations = KVStore::new(
            base_path.join("current_materialisations.kv"),
            "current_materialisations".to_string(),
        );
        let current_observations = KVStore::new(
            base_path.join("current_observations.kv"),
            "current_observations".to_string(),
        );
        let current_checks = KVStore::new(
            base_path.join("current_checks.kv"),
            "current_checks".to_string(),
        );
        let current_repository = KVStore::new(
            base_path.join("current_repository.kv"),
            "current_repository".to_string(),
        );
        let repositories = KVStore::new(
            base_path.join("repositories.kv"),
            "repositories".to_string(),
        );
        let connections = KVStore::new(base_path.join("connections.kv"), "connections".to_string());
        let current_reductions = KVStore::new(
            base_path.join("current_reductions.kv"),
            "current_reductions".to_string(),
        );

        let (
            known_blob_uids,
            known_file_uids,
            current_blobs,
            current_files,
            current_materialisations,
            current_observations,
            current_checks,
            current_repository,
            repositories,
            connections,
            current_reductions,
        ) = tokio::try_join!(
            known_blob_uids,
            known_file_uids,
            current_blobs,
            current_files,
            current_materialisations,
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
            current_blobs,
            current_files,
            current_materialisations,
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
            self.current_blobs.close(),
            self.current_files.close(),
            self.current_materialisations.close(),
            self.current_observations.close(),
            self.current_checks.close(),
            self.current_repository.close(),
            self.repositories.close(),
            self.connections.close(),
            self.current_reductions.close(),
        )?;
        Ok(())
    }

    pub(crate) async fn compact(&self) -> Result<(), DBError> {
        tokio::try_join!(
            self.known_blob_uids.compact(),
            self.known_file_uids.compact(),
            self.current_blobs.compact(),
            self.current_files.compact(),
            self.current_materialisations.compact(),
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
        s: BoxStream<'static, Result<Uid, DBError>>,
    ) -> Result<u64, DBError> {
        let s = s.map(|e| e.map(|u| (u, Some(())))).boxed();
        self.known_blob_uids.apply(s).await
    }

    pub(crate) async fn apply_known_file_uids(
        &self,
        s: BoxStream<'static, Result<Uid, DBError>>,
    ) -> Result<u64, DBError> {
        let s = s.map(|e| e.map(|u| (u, Some(())))).boxed();
        self.known_file_uids.apply(s).await
    }

    pub(crate) async fn apply_blobs(
        &self,
        s: BoxStream<'static, Result<Blob, DBError>>,
    ) -> Result<u64, DBError> {
        let s = s.map(move |r| r.map(UpsertBlob)).boxed();
        self.current_blobs.upsert(s).await
    }

    pub(crate) async fn apply_files(
        &self,
        s: BoxStream<'static, Result<File, DBError>>,
    ) -> Result<u64, DBError> {
        let s = s.map(move |r| r.map(UpsertFile)).boxed();
        self.current_files.upsert(s).await
    }

    pub(crate) async fn apply_materialisations(
        &self,
        s: BoxStream<'static, Result<InsertMaterialisation, DBError>>,
    ) -> Result<u64, DBError> {
        let s = s.map(move |r| r.map(UpsertMaterialisation)).boxed();
        self.current_materialisations.upsert(s).await
    }
    
    pub(crate) async fn apply_file_seen(
        &self,
        s: BoxStream<'static, Result<FileSeen, DBError>>,
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
        let s = s.map(move |r| r.map(map)).boxed();
        self.current_observations.apply(s).await
    }

    pub(crate) async fn apply_file_checks(
        &self,
        s: BoxStream<'static, Result<FileCheck, DBError>>,
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
        let s = s.map(move |r| r.map(map)).boxed();
        self.current_checks.apply(s).await
    }

    pub(crate) async fn store_current_repository(
        &self,
        current_repository: CurrentRepository,
    ) -> Result<u64, DBError> {
        self.current_repository
            .apply(stream::iter([Ok(((), Some(current_repository)))]).boxed())
            .await
    }

    pub(crate) async fn apply_current_reductions(
        &self,
        s: BoxStream<'static, (TableName, LogOffset)>,
    ) -> Result<u64, DBError> {
        let s = s.map(|(t, o)| Ok(UpsertCurrentReduction(t, o))).boxed();
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
            .apply(
                stream::iter([Ok((
                    name,
                    Some(ConnectionMetadata {
                        connection_type,
                        parameter,
                    }),
                ))])
                .boxed(),
            )
            .await
    }

    pub(crate) async fn apply_repositories(
        &self,
        s: BoxStream<'_, Result<Repository, DBError>>,
    ) -> Result<u64, DBError> {
        let s = s.map(move |r| r.map(UpsertRepositoryMetadata)).boxed();
        self.repositories.upsert(s).await
    }

    pub(crate) fn stream_current_blobs(
        &self,
    ) -> BoxStream<'static, Result<(BlobRef, CurrentBlob), DBError>> {
        self.current_blobs.stream()
    }

    pub(crate) fn stream_current_files(
        &self,
    ) -> BoxStream<'static, Result<(Path, CurrentFile), DBError>> {
        self.current_files.stream()
    }

    pub(crate) async fn load_current_repository(
        &self,
    ) -> Result<Option<CurrentRepository>, DBError> {
        let v: Vec<_> = self.current_repository.stream().try_collect().await?;
        Ok(v.into_iter().next().map(|(_, cr)| cr))
    }

    pub(crate) fn stream_repositories(
        &self,
    ) -> BoxStream<'static, Result<(RepoID, RepositoryMetadata), DBError>> {
        self.repositories.stream()
    }

    pub(crate) fn stream_connections(
        &self,
    ) -> BoxStream<'static, Result<(ConnectionName, ConnectionMetadata), DBError>> {
        self.connections.stream()
    }

    pub(crate) fn stream_current_reductions(
        &self,
    ) -> BoxStream<'static, Result<(TableName, LogOffset), DBError>> {
        self.current_reductions.stream()
    }

    pub(crate) fn left_join_current_blobs<IK, KF>(
        &self,
        s: BoxStream<'static, Result<IK, DBError>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<CurrentBlob>), DBError>>
    where
        KF: Fn(IK) -> BlobRef + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.current_blobs.left_join(s, key_func)
    }

    pub(crate) fn left_join_known_blob_uids<IK, KF>(
        &self,
        s: BoxStream<'static, Result<IK, DBError>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<()>), DBError>>
    where
        KF: Fn(IK) -> Uid + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.known_blob_uids.left_join(s, key_func)
    }

    pub(crate) fn left_join_known_file_uids<IK, KF>(
        &self,
        s: BoxStream<'static, Result<IK, DBError>>,
        key_func: KF,
    ) -> BoxStream<'_, Result<(IK, Option<()>), DBError>>
    where
        KF: Fn(IK) -> Uid + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.known_file_uids.left_join(s, key_func)
    }

    pub(crate) fn left_join_current_files<
        IK,
        KF,
        E: From<DBError> + Debug + Send + Sync + 'static,
    >(
        &self,
        s: BoxStream<'static, Result<IK, E>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<CurrentFile>), E>>
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
        s: BoxStream<'static, Result<IK, E>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<CurrentMaterialisation>), E>>
    where
        KF: Fn(IK) -> Path + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.current_materialisations.left_join(s, key_func)
    }

    pub(crate) fn left_join_current_observations<
        IK,
        KF,
        E: From<DBError> + Debug + Send + Sync + 'static,
    >(
        &self,
        s: BoxStream<'static, Result<IK, E>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<CurrentObservation>), E>>
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
        s: BoxStream<'static, Result<IK, E>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<CurrentCheck>), E>>
    where
        KF: Fn(IK) -> Path + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.current_checks.left_join(s, key_func)
    }

    pub(crate) fn left_join_repositories<IK, KF, E: From<DBError> + Debug + Send + Sync + 'static>(
        &self,
        s: BoxStream<'static, Result<IK, E>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<RepositoryMetadata>), E>>
    where
        KF: Fn(IK) -> RepoID + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.repositories.left_join(s, key_func)
    }

    pub(crate) fn left_join_connections<IK, KF, E: From<DBError> + Debug + Send + Sync + 'static>(
        &self,
        s: BoxStream<'static, Result<IK, E>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<ConnectionMetadata>), E>>
    where
        KF: Fn(IK) -> ConnectionName + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.connections.left_join(s, key_func)
    }
}
