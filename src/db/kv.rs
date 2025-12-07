use crate::db::error::DBError;
use crate::db::models::{
    Connection, ConnectionMetadata, ConnectionName, CurrentCheck, CurrentObservation, FileCheck,
    FileSeen, InsertMaterialisation, LocalRepository, Materialisation, Path, RepoID, Repository,
    RepositoryMetadata,
};
use crate::db::stores::kv::{Store, Upsert, UpsertAction};
use crate::flightdeck::tracer::Tracer;
use futures::{StreamExt, TryStreamExt, stream};
use futures_core::stream::BoxStream;
use std::fmt::Debug;
use std::path::PathBuf;
use tokio::fs::create_dir_all;

struct UpsertMaterialisation(InsertMaterialisation);
impl Upsert for UpsertMaterialisation {
    type K = Path;
    type V = Materialisation;

    fn key(&self) -> Path {
        self.0.path.clone()
    }

    fn upsert(self, v: Option<Materialisation>) -> UpsertAction<Materialisation> {
        if let Some(v) = v
            && v.valid_from > self.0.valid_from
        {
            return UpsertAction::NoChange;
        }

        match self.0.blob_id {
            Some(blob_id) => UpsertAction::Change(Materialisation {
                blob_id,
                valid_from: self.0.valid_from,
            }),
            None => UpsertAction::Delete,
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
    materialisations: Store<Path, Materialisation>,
    observations: Store<Path, CurrentObservation>,
    checks: Store<Path, CurrentCheck>,
    local_repository: Store<(), LocalRepository>,
    repositories: Store<RepoID, RepositoryMetadata>,
    connections: Store<ConnectionName, ConnectionMetadata>,
}

impl KVStores {
    pub(crate) async fn new(base_path: PathBuf) -> Result<Self, DBError> {
        create_dir_all(&base_path).await?;

        let tracer = Tracer::new_on("KVStores::new");
        let materialisations = Store::new(
            base_path.join("materialisations.kv"),
            "materialisations".to_string(),
        );
        let observations = Store::new(
            base_path.join("observations.kv"),
            "observations".to_string(),
        );
        let checks = Store::new(base_path.join("checks.kv"), "checks".to_string());
        let local_repository = Store::new(
            base_path.join("local_repository.kv"),
            "local_repository".to_string(),
        );
        let repositories = Store::new(
            base_path.join("repositories.kv"),
            "repositories".to_string(),
        );
        let connections = Store::new(base_path.join("connections.kv"), "connections".to_string());

        let (materialisations, observations, checks, local_repository, repositories, connections) =
            tokio::try_join!(
                materialisations,
                observations,
                checks,
                local_repository,
                repositories,
                connections,
            )?;
        tracer.measure();

        Ok(Self {
            materialisations,
            observations,
            checks,
            local_repository,
            repositories,
            connections,
        })
    }

    pub(crate) async fn close(&self) -> Result<(), DBError> {
        tokio::try_join!(
            self.materialisations.close(),
            self.observations.close(),
            self.checks.close(),
            self.local_repository.close(),
            self.repositories.close(),
            self.connections.close(),
        )?;
        Ok(())
    }

    pub(crate) async fn compact(&self) -> Result<(), DBError> {
        tokio::try_join!(
            self.materialisations.compact(),
            self.observations.compact(),
            self.checks.compact(),
            self.local_repository.compact(),
            self.repositories.compact(),
            self.connections.compact(),
        )?;
        Ok(())
    }

    pub(crate) async fn apply_materialisations(
        &self,
        s: BoxStream<'_, Result<InsertMaterialisation, DBError>>,
    ) -> Result<u64, DBError> {
        let s = s.map(move |r| r.map(UpsertMaterialisation)).boxed();
        self.materialisations.upsert(s).await
    }

    pub(crate) async fn apply_file_seen(
        &self,
        s: BoxStream<'_, Result<FileSeen, DBError>>,
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
        self.observations.apply(s).await
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
        self.checks.apply(s).await
    }

    pub(crate) async fn store_current_repository(
        &self,
        current_repository: LocalRepository,
    ) -> Result<u64, DBError> {
        self.local_repository
            .apply(stream::iter([Ok(((), Some(current_repository)))]).boxed())
            .await
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

    pub(crate) async fn load_local_repository(&self) -> Result<Option<LocalRepository>, DBError> {
        let v: Vec<_> = self.local_repository.stream().try_collect().await?;
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

    pub(crate) fn left_join_materialisations<
        IK,
        KF,
        E: From<DBError> + Debug + Send + Sync + 'static,
    >(
        &self,
        s: BoxStream<'static, Result<IK, E>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<Materialisation>), E>>
    where
        KF: Fn(IK) -> Path + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.materialisations.left_join(s, key_func)
    }

    pub(crate) fn left_join_observations<IK, KF, E: From<DBError> + Debug + Send + Sync + 'static>(
        &self,
        s: BoxStream<'static, Result<IK, E>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<CurrentObservation>), E>>
    where
        KF: Fn(IK) -> Path + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.observations.left_join(s, key_func)
    }

    pub(crate) fn left_join_check<IK, KF, E: From<DBError> + Debug + Send + Sync + 'static>(
        &self,
        s: BoxStream<'static, Result<IK, E>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<CurrentCheck>), E>>
    where
        KF: Fn(IK) -> Path + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.checks.left_join(s, key_func)
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
