use crate::db::error::DBError;
use crate::db::models::{
    Check, Connection, ConnectionMetadata, ConnectionName, FileCheck, FileSeen,
    InsertMaterialisation, LocalRepository, Materialisation, Observation, Path, RepoID,
    RepositorySyncState, SyncState,
};
use crate::db::stores::kv::{Store, Upsert, UpsertAction};
use crate::db::versioning::V1;
use crate::flightdeck::tracer::Tracer;
use futures::{StreamExt, TryStreamExt, stream};
use futures_core::stream::BoxStream;
use std::fmt::Debug;
use std::path::PathBuf;
use tokio::fs::create_dir_all;

struct UpsertRepositoryMetadata(RepositorySyncState);
impl Upsert for UpsertRepositoryMetadata {
    type K = RepoID;
    type V = V1<SyncState>;

    fn key(&self) -> RepoID {
        self.0.repo_id.clone()
    }

    fn upsert(self, v: Option<V1<SyncState>>) -> UpsertAction<V1<SyncState>> {
        let merge = |l, r| match (l, r) {
            (Some(l), Some(r)) => Some(std::cmp::max(l, r)),
            (Some(l), None) => Some(l),
            (None, Some(r)) => Some(r),
            (None, None) => None,
        };
        match v {
            Some(v) => UpsertAction::Change(
                SyncState {
                    last_file_index: merge(v.last_file_index, self.0.last_file_index),
                    last_blob_index: merge(v.last_blob_index, self.0.last_blob_index),
                    last_name_index: merge(v.last_name_index, self.0.last_name_index),
                }
                .into(),
            ),
            None => UpsertAction::Change(
                SyncState {
                    last_file_index: self.0.last_file_index,
                    last_blob_index: self.0.last_blob_index,
                    last_name_index: self.0.last_name_index,
                }
                .into(),
            ),
        }
    }
}

#[derive(Clone)]
pub(crate) struct KVStores {
    materialisations: Store<Path, V1<Materialisation>>,
    observations: Store<Path, V1<Observation>>,
    checks: Store<Path, V1<Check>>,
    local_repository: Store<(), V1<LocalRepository>>,
    sync_states: Store<RepoID, V1<SyncState>>,
    connections: Store<ConnectionName, V1<ConnectionMetadata>>,
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
        let sync_states = Store::new(base_path.join("sync_states.kv"), "sync_states".to_string());
        let connections = Store::new(base_path.join("connections.kv"), "connections".to_string());

        let (materialisations, observations, checks, local_repository, sync_states, connections) =
            tokio::try_join!(
                materialisations,
                observations,
                checks,
                local_repository,
                sync_states,
                connections,
            )?;
        tracer.measure();

        Ok(Self {
            materialisations,
            observations,
            checks,
            local_repository,
            sync_states,
            connections,
        })
    }

    pub(crate) async fn close(&self) -> Result<(), DBError> {
        tokio::try_join!(
            self.materialisations.close(),
            self.observations.close(),
            self.checks.close(),
            self.local_repository.close(),
            self.sync_states.close(),
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
            self.sync_states.compact(),
            self.connections.compact(),
        )?;
        Ok(())
    }

    pub(crate) async fn apply_materialisations(
        &self,
        s: BoxStream<'_, Result<InsertMaterialisation, DBError>>,
    ) -> Result<u64, DBError> {
        let map = |m: InsertMaterialisation| {
            (
                m.path,
                m.blob_id.map(|blob_id| Materialisation { blob_id }.into()),
            )
        };
        let s = s.map(move |r| r.map(map)).boxed();
        self.materialisations.apply(s).await
    }

    pub(crate) async fn apply_file_seen(
        &self,
        s: BoxStream<'_, Result<FileSeen, DBError>>,
    ) -> Result<u64, DBError> {
        let map = |e: FileSeen| {
            (
                e.path,
                Some(
                    Observation {
                        fs_last_seen_id: e.seen_id,
                        fs_last_seen_dttm: e.seen_dttm,
                        fs_last_modified_dttm: e.last_modified_dttm,
                        fs_last_size: e.size,
                    }
                    .into(),
                ),
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
                Some(
                    Check {
                        check_last_dttm: e.check_dttm,
                        check_last_hash: e.hash,
                    }
                    .into(),
                ),
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
            .apply(stream::iter([Ok(((), Some(current_repository.into())))]).boxed())
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
                    Some(
                        ConnectionMetadata {
                            connection_type,
                            parameter,
                        }
                        .into(),
                    ),
                ))])
                .boxed(),
            )
            .await
    }

    pub(crate) async fn apply_repository_sync_states(
        &self,
        s: BoxStream<'_, Result<RepositorySyncState, DBError>>,
    ) -> Result<u64, DBError> {
        let s = s.map(move |r| r.map(UpsertRepositoryMetadata)).boxed();
        self.sync_states.upsert(s).await
    }

    pub(crate) async fn load_local_repository(&self) -> Result<Option<LocalRepository>, DBError> {
        let v: Vec<_> = self.local_repository.stream().try_collect().await?;
        Ok(v.into_iter().next().map(|(_, cr)| cr.into_inner()))
    }

    pub(crate) fn stream_repository_sync_states(
        &self,
    ) -> BoxStream<'static, Result<(RepoID, SyncState), DBError>> {
        self.sync_states
            .stream()
            .map(|r| r.map(|(a, b)| (a, b.into_inner())))
            .boxed()
    }

    pub(crate) fn stream_connections(
        &self,
    ) -> BoxStream<'static, Result<(ConnectionName, ConnectionMetadata), DBError>> {
        self.connections
            .stream()
            .map(|r| r.map(|(a, b)| (a, b.into_inner())))
            .boxed()
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
        self.materialisations
            .left_join(s, key_func)
            .map(|r| r.map(|(a, b)| (a, b.map(V1::into_inner))))
            .boxed()
    }

    pub(crate) fn left_join_observations<IK, KF, E: From<DBError> + Debug + Send + Sync + 'static>(
        &self,
        s: BoxStream<'static, Result<IK, E>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<Observation>), E>>
    where
        KF: Fn(IK) -> Path + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.observations
            .left_join(s, key_func)
            .map(|r| r.map(|(a, b)| (a, b.map(V1::into_inner))))
            .boxed()
    }

    pub(crate) fn left_join_check<IK, KF, E: From<DBError> + Debug + Send + Sync + 'static>(
        &self,
        s: BoxStream<'static, Result<IK, E>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<Check>), E>>
    where
        KF: Fn(IK) -> Path + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.checks
            .left_join(s, key_func)
            .map(|r| r.map(|(a, b)| (a, b.map(V1::into_inner))))
            .boxed()
    }

    pub(crate) fn left_join_repository_sync_states<
        IK,
        KF,
        E: From<DBError> + Debug + Send + Sync + 'static,
    >(
        &self,
        s: BoxStream<'static, Result<IK, E>>,
        key_func: KF,
    ) -> BoxStream<'static, Result<(IK, Option<SyncState>), E>>
    where
        KF: Fn(IK) -> RepoID + Sync + Send + 'static,
        IK: Clone + Send + Sync + 'static,
    {
        self.sync_states
            .left_join(s, key_func)
            .map(|r| r.map(|(a, b)| (a, b.map(V1::into_inner))))
            .boxed()
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
        self.connections
            .left_join(s, key_func)
            .map(|r| r.map(|(a, b)| (a, b.map(V1::into_inner))))
            .boxed()
    }
}
