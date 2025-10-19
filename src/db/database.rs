use crate::db::cleaner::Cleaner;
use crate::db::error::DBError;
use crate::db::models::{
    AvailableBlob, Blob, BlobAssociatedToFiles, BlobTransferItem, Connection, CopiedTransferItem,
    CurrentRepository, File, FileTransferItem, InsertBlob, InsertFile, InsertMaterialisation,
    InsertRepositoryName, MaterialisationProjection, MissingFile, MoveEvent, MoveInstr,
    MoveViolation, MoveViolationCode, Observation, ObservedBlob, PathType, Repository,
    RepositoryName, RmEvent, RmInstr, RmViolation, RmViolationCode, VirtualFile,
};
use crate::db::redb_history::{
    BlobRecord as RedbBlobRecord, FileRecord as RedbFileRecord,
    MaterialisationRecord as RedbMaterialisationRecord, RedbHistoryStore,
    TransferRecord as RedbTransferRecord,
};
use crate::db::redb_store::RedbStore;
use crate::db::virtual_filesystem::VirtualFilesystemStore;
use crate::flightdeck::tracked::stream::Trackable;
use crate::utils::flow::{ExtFlow, Flow};
use crate::utils::stream::BoundedWaitChunksExt;
use async_stream::try_stream;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, stream};
use log::debug;
use sqlx::query::Query;
use sqlx::sqlite::SqliteArguments;
use sqlx::{Either, Executor, FromRow, Sqlite, SqlitePool, query};
use std::collections::{BTreeMap, HashMap, HashSet};
use uuid::Uuid;

#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
    max_variable_number: usize,
    cleaner: Cleaner,
    virtual_filesystem: VirtualFilesystemStore,
    redb: RedbStore,
    redb_history: RedbHistoryStore,
}

const TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_millis(5);

pub(crate) type DBOutputStream<'a, T> = BoxStream<'a, Result<T, DBError>>;

impl Database {
    pub fn new(pool: SqlitePool, virtual_filesystem: VirtualFilesystemStore) -> Self {
        let cleaner = Cleaner::new(pool.clone());
        let redb = virtual_filesystem.redb_store();
        let redb_history = RedbHistoryStore::new(redb.clone());
        Self {
            pool,
            max_variable_number: 32000, // 32766 - actually: https://sqlite.org/limits.html
            cleaner,
            virtual_filesystem,
            redb,
            redb_history,
        }
    }

    pub async fn clean(&self) -> Result<(), DBError> {
        self.redb_history.clear_transfers().await?;

        let _ = self.cleaner.try_periodic_cleanup().await;

        Ok(())
    }

    fn stream<'a, T>(
        &self,
        name: &str,
        q: Query<'static, Sqlite, SqliteArguments<'static>>,
    ) -> DBOutputStream<'a, T>
    where
        T: Send + Unpin + for<'r> FromRow<'r, <Sqlite as sqlx::Database>::Row> + 'static,
    {
        let pool = self.pool.clone();
        let cleaner = self.cleaner.clone();
        let stream = try_stream! {
            let _long_running_stream_guard = cleaner.try_periodic_cleanup().await;

            let mut rows = pool.fetch_many(q);
            while let Some(item) = rows.next().await {
                match item {
                    Ok(Either::Right(row)) => {
                        yield T::from_row(&row)?;
                    }
                    Ok(Either::Left(_)) => continue,
                    Err(e) => {
                        log::error!("Database::stream failed: {e}");
                        Err(e)?
                    },
                }
            }
        };

        stream.boxed().track(name).boxed()
    }

    pub async fn get_or_create_current_repository(&self) -> Result<CurrentRepository, DBError> {
        let potential_new_repository_id = Uuid::new_v4().to_string();
        if let Some(repo) = sqlx::query_as::<_, CurrentRepository>(
            "INSERT OR IGNORE INTO current_repository (id, repo_id) VALUES (1, ?)
            RETURNING repo_id;",
        )
        .bind(potential_new_repository_id)
        .fetch_optional(&self.pool)
        .await
        .inspect_err(|e| log::error!("Database::get_or_create_current_repository failed: {e}"))?
        {
            return Ok(repo);
        }
        sqlx::query_as::<_, CurrentRepository>("SELECT id, repo_id FROM current_repository LIMIT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(DBError::from)
    }

    pub async fn lookup_current_repository_name(
        &self,
        repo_id: String,
    ) -> Result<Option<String>, DBError> {
        #[derive(Debug, FromRow)]
        struct Name {
            name: String,
        }

        Ok(
            sqlx::query_as::<_, Name>(
                "SELECT name FROM latest_repository_names WHERE repo_id = ?;",
            )
            .bind(repo_id)
            .fetch_optional(&self.pool)
            .await
            .inspect_err(|e| log::error!("Database::lookup_current_repository_name failed: {e}"))?
            .map(|n| n.name),
        )
    }

    pub async fn add_files<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = InsertFile> + Send + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s
            .bounded_wait_chunks(self.max_variable_number / 4, TIMEOUT)
            .boxed();

        while let Some(chunk) = chunk_stream.next().await {
            let _cleanup_guard = self.cleaner.periodic_cleanup(chunk.len()).await;
            if chunk.is_empty() {
                continue;
            }

            let redb_records: Vec<_> = chunk
                .iter()
                .map(|file| RedbFileRecord {
                    id: 0,
                    uuid: Uuid::new_v4().to_string(),
                    path: file.path.clone(),
                    blob_id: file.blob_id.clone(),
                    valid_from: file.valid_from,
                })
                .collect();

            if redb_records.is_empty() {
                continue;
            }

            total_inserted += redb_records.len() as u64;
            total_attempted += redb_records.len() as u64;

            self.redb_history.append_files(redb_records).await?;
        }

        debug!(
            "files added: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(total_inserted)
    }

    pub async fn add_blobs<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = InsertBlob> + Send + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s
            .bounded_wait_chunks(self.max_variable_number / 7, TIMEOUT)
            .boxed();

        while let Some(chunk) = chunk_stream.next().await {
            let _cleanup_guard = self.cleaner.periodic_cleanup(chunk.len()).await;
            if chunk.is_empty() {
                continue;
            }

            let redb_records: Vec<_> = chunk
                .iter()
                .map(|blob| RedbBlobRecord {
                    id: 0,
                    uuid: Uuid::new_v4().to_string(),
                    repo_id: blob.repo_id.clone(),
                    blob_id: blob.blob_id.clone(),
                    blob_size: blob.blob_size,
                    has_blob: blob.has_blob,
                    path: blob.path.clone(),
                    valid_from: blob.valid_from,
                })
                .collect();

            if redb_records.is_empty() {
                continue;
            }

            total_attempted += redb_records.len() as u64;
            total_inserted += redb_records.len() as u64;

            self.redb_history.append_blobs(redb_records).await?;
        }

        debug!(
            "blobs added: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(total_inserted)
    }

    pub async fn observe_blobs<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = ObservedBlob> + Send + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s
            .bounded_wait_chunks(self.max_variable_number / 5, TIMEOUT)
            .boxed();

        while let Some(chunk) = chunk_stream.next().await {
            let _cleanup_guard = self.cleaner.periodic_cleanup(chunk.len()).await;
            if chunk.is_empty() {
                continue;
            }

            total_attempted += chunk.len() as u64;

            let mut grouped: HashMap<String, Vec<ObservedBlob>> = HashMap::new();
            for observation in &chunk {
                grouped
                    .entry(observation.repo_id.clone())
                    .or_default()
                    .push(observation.clone());
            }

            let mut redb_records = Vec::new();
            for (repo_id, observations) in grouped.into_iter() {
                let paths: Vec<String> = observations.iter().map(|obs| obs.path.clone()).collect();
                let latest = self
                    .redb_history
                    .latest_available_blobs_for_repo_paths(&repo_id, &paths)
                    .await?;

                for observation in observations {
                    if let Some(existing) = latest.get(&observation.path) {
                        redb_records.push(RedbBlobRecord {
                            id: 0,
                            uuid: Uuid::new_v4().to_string(),
                            repo_id: repo_id.clone(),
                            blob_id: existing.blob_id.clone(),
                            blob_size: existing.blob_size,
                            has_blob: observation.has_blob,
                            path: existing.path.clone(),
                            valid_from: observation.valid_from,
                        });
                    }
                }
            }

            total_inserted += redb_records.len() as u64;

            if !redb_records.is_empty() {
                self.redb_history.append_blobs(redb_records).await?;
            }
        }

        debug!(
            "blobs added: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(total_inserted)
    }

    pub async fn add_repository_names<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = InsertRepositoryName> + Send + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s
            .bounded_wait_chunks(self.max_variable_number / 4, TIMEOUT)
            .boxed();

        while let Some(chunk) = chunk_stream.next().await {
            let _cleanup_guard = self.cleaner.periodic_cleanup(chunk.len()).await;
            if chunk.is_empty() {
                continue;
            }

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT INTO repository_names (uuid, repo_id, name, valid_from) VALUES {}",
                placeholders
            );

            let mut query = sqlx::query(&query_str);

            for repository_name in &chunk {
                let uuid = Uuid::new_v4().to_string();
                query = query
                    .bind(uuid)
                    .bind(&repository_name.repo_id)
                    .bind(&repository_name.name)
                    .bind(repository_name.valid_from);
            }

            let result = query
                .execute(&self.pool)
                .await
                .inspect_err(|e| log::error!("Database::add_repository_names failed: {e}"))?;
            total_inserted += result.rows_affected();
            total_attempted += chunk.len() as u64;
        }

        debug!(
            "repository_names added: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(total_inserted)
    }

    pub async fn select_repositories(&self) -> DBOutputStream<'static, Repository> {
        self.stream("Database::select_repositories", query(
            "SELECT repo_id, last_file_index, last_blob_index, last_name_index FROM repositories",
        ))
    }

    pub async fn select_files(&self, last_index: i32) -> DBOutputStream<'static, File> {
        let cleaner = self.cleaner.clone();
        let redb_history = self.redb_history.clone();
        let start = last_index as i64;

        let stream = try_stream! {
            let _guard = cleaner.try_periodic_cleanup().await;

            let records = redb_history.files_after(start).await?;
            for record in records {
                yield File {
                    uuid: record.uuid,
                    path: record.path,
                    blob_id: record.blob_id,
                    valid_from: record.valid_from,
                };
            }
        };

        stream.boxed().track("Database::select_files").boxed()
    }

    pub async fn select_blobs(&self, last_index: i32) -> DBOutputStream<'static, Blob> {
        let cleaner = self.cleaner.clone();
        let redb_history = self.redb_history.clone();
        let start = last_index as i64;

        let stream = try_stream! {
            let _guard = cleaner.try_periodic_cleanup().await;

            let records = redb_history.blobs_after(start).await?;
            for record in records {
                yield Blob {
                    uuid: record.uuid,
                    repo_id: record.repo_id,
                    blob_id: record.blob_id,
                    blob_size: record.blob_size,
                    has_blob: record.has_blob,
                    path: record.path,
                    valid_from: record.valid_from,
                };
            }
        };

        stream.boxed().track("Database::select_blobs").boxed()
    }

    pub async fn select_repository_names(
        &self,
        last_index: i32,
    ) -> DBOutputStream<'static, RepositoryName> {
        self.stream(
            "Database::select_repository_names",
            query(
                "
                SELECT uuid, repo_id, name, valid_from
                FROM repository_names
                WHERE id > ?",
            )
            .bind(last_index),
        )
    }

    pub async fn merge_repositories<S>(&self, s: S) -> Result<(), DBError>
    where
        S: Stream<Item = Repository> + Unpin + Send,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s
            .bounded_wait_chunks(self.max_variable_number / 3, TIMEOUT)
            .boxed();

        while let Some(chunk) = chunk_stream.next().await {
            let _cleanup_guard = self.cleaner.periodic_cleanup(chunk.len()).await;
            if chunk.is_empty() {
                continue;
            }

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT INTO repositories (repo_id, last_file_index, last_blob_index, last_name_index)
                 VALUES {}
                     ON CONFLICT(repo_id) DO UPDATE SET
                    last_file_index = max(last_file_index, excluded.last_file_index),
                    last_blob_index = max(last_blob_index, excluded.last_blob_index),
                    last_name_index = max(last_name_index, excluded.last_name_index)
                ",
                placeholders
            );

            let mut query = sqlx::query(&query_str);

            for repo in &chunk {
                query = query
                    .bind(&repo.repo_id)
                    .bind(repo.last_file_index)
                    .bind(repo.last_blob_index)
                    .bind(repo.last_name_index)
            }

            let result = query
                .execute(&self.pool)
                .await
                .inspect_err(|e| log::error!("Database::merge_repositories failed: {e}"))?;
            total_inserted += result.rows_affected();
            total_attempted += chunk.len() as u64;
        }

        debug!(
            "repositories merged: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(())
    }

    pub async fn merge_files<S>(&self, s: S) -> Result<(), DBError>
    where
        S: Stream<Item = File> + Unpin + Send,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s
            .bounded_wait_chunks(self.max_variable_number / 4, TIMEOUT)
            .boxed();

        while let Some(chunk) = chunk_stream.next().await {
            let _cleanup_guard = self.cleaner.periodic_cleanup(chunk.len()).await;
            if chunk.is_empty() {
                continue;
            }

            total_attempted += chunk.len() as u64;

            let redb_records: Vec<_> = chunk
                .iter()
                .map(|file| RedbFileRecord {
                    id: 0,
                    uuid: file.uuid.clone(),
                    path: file.path.clone(),
                    blob_id: file.blob_id.clone(),
                    valid_from: file.valid_from,
                })
                .collect();

            let inserted = self.redb_history.merge_files(redb_records).await?;
            total_inserted += inserted as u64;
        }

        debug!(
            "files merged: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(())
    }

    pub async fn merge_blobs<S>(&self, s: S) -> Result<(), DBError>
    where
        S: Stream<Item = Blob> + Unpin + Send,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s
            .bounded_wait_chunks(self.max_variable_number / 7, TIMEOUT)
            .boxed();

        while let Some(chunk) = chunk_stream.next().await {
            let _cleanup_guard = self.cleaner.periodic_cleanup(chunk.len()).await;
            if chunk.is_empty() {
                continue;
            }

            total_attempted += chunk.len() as u64;

            let redb_records: Vec<_> = chunk
                .iter()
                .map(|blob| RedbBlobRecord {
                    id: 0,
                    uuid: blob.uuid.clone(),
                    repo_id: blob.repo_id.clone(),
                    blob_id: blob.blob_id.clone(),
                    blob_size: blob.blob_size,
                    has_blob: blob.has_blob,
                    path: blob.path.clone(),
                    valid_from: blob.valid_from,
                })
                .collect();

            let inserted = self.redb_history.merge_blobs(redb_records).await?;
            total_inserted += inserted as u64;
        }

        debug!(
            "blobs merged: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(())
    }

    pub async fn merge_repository_names<S>(&self, s: S) -> Result<(), DBError>
    where
        S: Stream<Item = RepositoryName> + Unpin + Send,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s
            .bounded_wait_chunks(self.max_variable_number / 4, TIMEOUT)
            .boxed();

        while let Some(chunk) = chunk_stream.next().await {
            let _cleanup_guard = self.cleaner.periodic_cleanup(chunk.len()).await;
            if chunk.is_empty() {
                continue;
            }

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT OR IGNORE INTO repository_names (uuid, repo_id, name, valid_from) VALUES {}",
                placeholders
            );

            let mut query = sqlx::query(&query_str);

            for file in &chunk {
                query = query
                    .bind(&file.uuid)
                    .bind(&file.repo_id)
                    .bind(&file.name)
                    .bind(file.valid_from)
            }

            let result = query
                .execute(&self.pool)
                .await
                .inspect_err(|e| log::error!("Database::merge_repository_names failed: {e}"))?;
            total_inserted += result.rows_affected();
            total_attempted += chunk.len() as u64;
        }

        debug!(
            "repository_names merged: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(())
    }

    pub async fn lookup_repository(&self, repo_id: String) -> Result<Repository, DBError> {
        sqlx::query_as::<_, Repository>(
            "
                SELECT COALESCE(r.repo_id, ?) as repo_id,
                       COALESCE(r.last_file_index, -1) as last_file_index,
                       COALESCE(r.last_blob_index, -1) as last_blob_index,
                       COALESCE(r.last_name_index, -1) as last_name_index
                FROM (SELECT NULL) n
                         LEFT JOIN repositories r ON r.repo_id = ?
                LIMIT 1
            ",
        )
        .bind(&repo_id)
        .bind(&repo_id)
        .fetch_one(&self.pool)
        .await
        .map_err(DBError::from)
    }

    pub async fn update_last_indices(&self) -> Result<Repository, DBError> {
        let last_file_index = self.redb_history.current_file_sequence().await?;
        let last_blob_index = self.redb_history.current_blob_sequence().await?;

        // TODO
        sqlx::query_as::<_, Repository>(
            "
            INSERT INTO repositories (repo_id, last_file_index, last_blob_index, last_name_index)
            VALUES (
                (SELECT repo_id FROM current_repository LIMIT 1),
                ?,
                ?,
                (SELECT COALESCE(MAX(id), -1) FROM repository_names)
            )
            ON CONFLICT(repo_id) DO UPDATE
            SET
                last_file_index = MAX(excluded.last_file_index, repositories.last_file_index),
                last_blob_index = MAX(excluded.last_blob_index, repositories.last_blob_index),
                last_name_index = MAX(excluded.last_name_index, repositories.last_name_index)
            RETURNING repo_id, last_file_index, last_blob_index, last_name_index;
            ",
        )
        .bind(last_file_index)
        .bind(last_blob_index)
        .fetch_one(&self.pool)
        .await
        .map_err(DBError::from)
    }

    pub(crate) fn available_blobs(
        &self,
        repo_id: String,
    ) -> impl Stream<Item = Result<AvailableBlob, DBError>> + Unpin + Send + Sized + 'static {
        let cleaner = self.cleaner.clone();
        let redb_history = self.redb_history.clone();
        let repo_id_for_query = repo_id;

        let stream = try_stream! {
            let _long_running_stream_guard = cleaner.try_periodic_cleanup().await;

            let blobs = redb_history
                .latest_available_blobs_for_repo(&repo_id_for_query)
                .await?;

            for blob in blobs.iter().cloned() {
                yield blob;
            }
        };

        stream.boxed().track("Database::available_blobs").boxed()
    }

    pub(crate) fn missing_blobs(
        &self,
        repo_id: String,
    ) -> impl Stream<Item = Result<BlobAssociatedToFiles, DBError>> + Unpin + Send + Sized + 'static
    {
        let pool = self.pool.clone();
        let cleaner = self.cleaner.clone();
        let redb_history = self.redb_history.clone();
        let repo_id_for_query = repo_id;

        let stream = try_stream! {
            let _long_running_stream_guard = cleaner.try_periodic_cleanup().await;

            // TODO: complete list
            let files_snapshot = redb_history
                .latest_filesystem_files_snapshot()
                .await?;
            let availability = redb_history.repositories_with_blob().await?;

            let mut blob_to_paths: HashMap<String, Vec<String>> = HashMap::new();
            for (path, blob_id) in files_snapshot {
                blob_to_paths.entry(blob_id).or_default().push(path);
            }

            let repo_names: HashMap<String, String> = match sqlx::query_as::<_, (String, String)>(
                "SELECT repo_id, name FROM latest_repository_names",
            )
            .fetch_all(&pool)
            .await
            {
                Ok(rows) => rows.into_iter().collect(),
                Err(e) => {
                    log::error!(
                        "Database::missing_blobs failed to fetch latest_repository_names snapshot: {e}"
                    );
                    HashMap::new()
                }
            };

            let mut results = Vec::new();
            for (blob_id, mut paths) in blob_to_paths {
                let repos = availability.get(&blob_id);

                if repos
                    .map_or(false, |repos| repos.iter().any(|repo| repo == &repo_id_for_query))
                {
                    continue;
                }

                paths.sort();

                let repo_list = repos
                    .map(|repos| {
                        let mut names: Vec<String> = repos
                            .iter()
                            .map(|repo| {
                                repo_names
                                    .get(repo)
                                    .cloned()
                                    .unwrap_or_else(|| repo.clone())
                            })
                            .collect();
                        names.sort();
                        names
                    })
                    .unwrap_or_default();

                results.push(BlobAssociatedToFiles {
                    blob_id: blob_id.clone(),
                    paths,
                    repositories_with_blob: repo_list,
                });
            }

            results.sort_by(|a, b| {
                a.paths
                    .cmp(&b.paths)
                    .then_with(|| a.blob_id.cmp(&b.blob_id))
            });

            for entry in results.into_iter() {
                yield entry;
            }
        };

        stream.boxed().track("Database::missing_blobs").boxed()
    }

    pub async fn add_materialisations<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = InsertMaterialisation> + Send + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s
            .bounded_wait_chunks(self.max_variable_number / 3, TIMEOUT)
            .boxed();

        while let Some(chunk) = chunk_stream.next().await {
            let _cleanup_guard = self.cleaner.periodic_cleanup(chunk.len()).await;
            if chunk.is_empty() {
                continue;
            }

            let redb_records: Vec<_> = chunk
                .into_iter()
                .map(|mat| RedbMaterialisationRecord::new(0, mat.path, mat.blob_id, mat.valid_from))
                .collect();
            let inserted_count = redb_records.len() as u64;
            self.redb_history
                .append_materialisations(redb_records)
                .await?;
            total_inserted += inserted_count;
            total_attempted += inserted_count;
        }

        debug!(
            "materialisations added: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(total_inserted)
    }

    pub async fn truncate_virtual_filesystem(&self) -> Result<(), DBError> {
        self.virtual_filesystem
            .truncate()
            .await
            .inspect_err(|e| log::error!("Database::truncate_virtual_filesystem failed: {e}"))
    }

    pub async fn refresh_virtual_filesystem(&self) -> Result<(), DBError> {
        let current_repository = self.get_or_create_current_repository().await?;
        let repo_id = current_repository.repo_id.clone();

        let files_snapshot = self.redb_history.latest_filesystem_files_snapshot().await?;
        let materialisations_snapshot =
            self.redb_history.latest_materialisations_snapshot().await?;
        let available_blobs = self
            .redb_history
            .latest_available_blobs_for_repo(&repo_id)
            .await?;

        let file_map: HashMap<String, String> = files_snapshot.into_iter().collect();
        let materialisation_map: HashMap<String, Option<String>> =
            materialisations_snapshot.into_iter().collect();
        let local_availability: HashMap<String, i64> = available_blobs
            .into_iter()
            .map(|blob| (blob.blob_id, blob.blob_size))
            .collect();

        let mut projections = Vec::new();

        for (path, blob_id) in &file_map {
            let materialised = materialisation_map.get(path).cloned().unwrap_or(None);
            let target_blob_size = local_availability.get(blob_id).copied();
            let local_has_blob = local_availability.contains_key(blob_id);

            projections.push(MaterialisationProjection {
                path: path.clone(),
                materialisation_last_blob_id: materialised,
                target_blob_id: Some(blob_id.clone()),
                target_blob_size,
                local_has_target_blob: local_has_blob,
            });
        }

        for (path, materialised_blob) in &materialisation_map {
            if file_map.contains_key(path) {
                continue;
            }

            projections.push(MaterialisationProjection {
                path: path.clone(),
                materialisation_last_blob_id: materialised_blob.clone(),
                target_blob_id: None,
                target_blob_size: None,
                local_has_target_blob: false,
            });
        }

        projections.sort_by(|a, b| a.path.cmp(&b.path));

        let s = stream::iter(
            projections
                .into_iter()
                .map(|projection| Ok::<MaterialisationProjection, DBError>(projection)),
        )
        .boxed();

        let batch_id: i64 = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        self.virtual_filesystem
            .update_materialisation_data(s, batch_id)
            .await
    }

    pub async fn cleanup_virtual_filesystem(&self, last_seen_id: i64) -> Result<(), DBError> {
        self.virtual_filesystem
            .cleanup(last_seen_id)
            .await
            .inspect_err(|e| log::error!("Database::cleanup_virtual_filesystem failed: {e}"))
    }

    pub async fn select_missing_files_on_virtual_filesystem(
        &self,
        last_seen_id: i64,
    ) -> DBOutputStream<'static, MissingFile> {
        self.virtual_filesystem.select_missing_files(last_seen_id)
    }

    pub async fn add_virtual_filesystem_observations(
        &self,
        input_stream: impl Stream<Item = Flow<Observation>> + Unpin + Send + 'static,
    ) -> impl Stream<Item = ExtFlow<Result<Vec<VirtualFile>, DBError>>> + Unpin + Send + 'static
    {
        let s = self.clone();
        input_stream
            .bounded_wait_chunks(self.max_variable_number / 7, TIMEOUT)
            .then(move |chunk: Vec<Flow<Observation>>| {
                let s = s.clone();
                Box::pin(async move {
                    // propagate shutdown if any item is a shutdown signal
                    let shutting_down = chunk.iter().any(|f| matches!(f, Flow::Shutdown));
                    let observations: Vec<Observation> = chunk
                        .into_iter()
                        .filter_map(|f| match f {
                            Flow::Data(o) => Some(o),
                            _ => None,
                        })
                        .collect();

                    let result = s.virtual_filesystem.apply_observations(observations).await;
                    if shutting_down {
                        ExtFlow::Shutdown(result)
                    } else {
                        ExtFlow::Data(result)
                    }
                })
            })
            .boxed()
            .track("Database::add_virtual_filesystem_observations")
    }

    pub async fn add_connection(&self, connection: &Connection) -> Result<(), DBError> {
        let query = "
            INSERT INTO connections (name, connection_type, parameter)
            VALUES (?, ?, ?)
        ";

        sqlx::query(query)
            .bind(&connection.name)
            .bind(&connection.connection_type)
            .bind(&connection.parameter)
            .execute(&self.pool)
            .await
            .inspect_err(|e| log::error!("Database::add_connection failed: {e}"))?;
        Ok(())
    }

    pub async fn connection_by_name(&self, name: &str) -> Result<Option<Connection>, DBError> {
        let query = "
            SELECT name, connection_type, parameter
            FROM connections
            WHERE name = ?
        ";

        sqlx::query_as::<_, Connection>(query)
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(DBError::from)
    }

    pub async fn list_all_connections(&self) -> Result<Vec<Connection>, DBError> {
        let query = "
            SELECT name, connection_type, parameter
            FROM connections
        ";

        sqlx::query_as::<_, Connection>(query)
            .fetch_all(&self.pool)
            .await
            .map_err(DBError::from)
    }

    pub(crate) async fn populate_missing_blobs_for_transfer(
        &self,
        transfer_id: u32,
        remote_repo_id: String,
        paths: Vec<String>,
    ) -> DBOutputStream<'static, BlobTransferItem> {
        let cleaner = self.cleaner.clone();
        let redb_history = self.redb_history.clone();
        let remote_repo = remote_repo_id;
        let path_filters = paths;
        let local_repo = match self.get_or_create_current_repository().await {
            Ok(repo) => repo.repo_id,
            Err(err) => {
                return stream::once(async move { Err(err) })
                    .boxed()
                    .track("Database::populate_missing_blobs_for_transfer")
                    .boxed();
            }
        };

        let stream = try_stream! {
            let _guard = cleaner.try_periodic_cleanup().await;

            let local_available = redb_history
                .latest_available_blobs_for_repo(&local_repo)
                .await?;
            let remote_available = redb_history
                .latest_available_blobs_for_repo(&remote_repo)
                .await?;
            let filesystem = redb_history.latest_filesystem_files_snapshot().await?;

            let local_set: HashSet<String> = local_available.into_iter().map(|b| b.blob_id).collect();
            let remote_map: HashMap<String, i64> = remote_available
                .into_iter()
                .map(|b| (b.blob_id, b.blob_size))
                .collect();

            let mut grouped: HashMap<String, String> = HashMap::new();
            for (path, blob_id) in filesystem {
                if !path_matches(&path, &path_filters) {
                    continue;
                }
                if local_set.contains(&blob_id) {
                    continue;
                }
                if !remote_map.contains_key(&blob_id) {
                    continue;
                }

                grouped
                    .entry(blob_id.clone())
                    .and_modify(|existing| {
                        if path < *existing {
                            *existing = path.clone();
                        }
                    })
                    .or_insert(path);
            }

            let mut records: Vec<RedbTransferRecord> = grouped
                .into_iter()
                .map(|(blob_id, _)| RedbTransferRecord {
                    transfer_id: transfer_id as i64,
                    blob_size: remote_map.get(&blob_id).copied().unwrap_or(0),
                    path: blob_storage_path(&blob_id),
                    blob_id,
                })
                .collect();

            if records.is_empty() {
                return;
            }

            records.sort_by(|a, b| a.blob_id.cmp(&b.blob_id));

            let mut items: Vec<BlobTransferItem> = Vec::with_capacity(records.len());
            for record in &records {
                items.push(BlobTransferItem {
                    transfer_id,
                    blob_id: record.blob_id.clone(),
                    path: record.path.clone(),
                });
            }

            redb_history.store_transfers(records).await?;

            for item in items {
                yield item;
            }
        };

        stream
            .boxed()
            .track("Database::populate_missing_blobs_for_transfer")
            .boxed()
    }

    pub(crate) async fn select_blobs_transfer(
        &self,
        input_stream: impl Stream<Item = CopiedTransferItem> + Unpin + Send + 'static,
    ) -> DBOutputStream<'static, BlobTransferItem> {
        let redb_history = self.redb_history.clone();
        input_stream
            .bounded_wait_chunks(self.max_variable_number / 2, TIMEOUT)
            .then(move |chunk: Vec<CopiedTransferItem>| {
                let redb_history = redb_history.clone();
                Box::pin(async move {
                    if chunk.is_empty() {
                        // SQL is otherwise not valid
                        return stream::iter(vec![]);
                    }

                    let keys: Vec<(i64, String)> = chunk
                        .into_iter()
                        .map(|row| (row.transfer_id as i64, row.path))
                        .collect();

                    stream::iter(match redb_history.lookup_transfers(&keys).await {
                        Ok(records) => records
                            .into_iter()
                            .map(|record| {
                                Ok(BlobTransferItem {
                                    transfer_id: record.transfer_id as u32,
                                    blob_id: record.blob_id,
                                    path: record.path,
                                })
                            })
                            .collect::<Vec<_>>(),
                        Err(e) => vec![Err(e)],
                    })
                })
            })
            .flatten()
            .boxed()
            .track("Database::select_blobs_transfer")
            .boxed()
    }

    pub(crate) async fn populate_missing_files_for_transfer(
        &self,
        transfer_id: u32,
        local_repo_id: String,
        remote_repo_id: String,
        paths: Vec<String>,
    ) -> DBOutputStream<'static, FileTransferItem> {
        let cleaner = self.cleaner.clone();
        let redb_history = self.redb_history.clone();
        let local_repo = local_repo_id;
        let remote_repo = remote_repo_id;
        let path_filters = paths;

        let stream = try_stream! {
            let _guard = cleaner.try_periodic_cleanup().await;

            let local_available = redb_history
                .latest_available_blobs_for_repo(&local_repo)
                .await?;
            let remote_available = redb_history
                .latest_available_blobs_for_repo(&remote_repo)
                .await?;
            let filesystem = redb_history.latest_filesystem_files_snapshot().await?;

            let local_set: HashSet<String> = local_available.into_iter().map(|b| b.blob_id).collect();
            let remote_map: HashMap<String, i64> = remote_available
                .into_iter()
                .map(|b| (b.blob_id, b.blob_size))
                .collect();

            let mut grouped: HashMap<String, String> = HashMap::new();
            for (path, blob_id) in filesystem {
                if !path_matches(&path, &path_filters) {
                    continue;
                }
                if local_set.contains(&blob_id) {
                    continue;
                }
                if !remote_map.contains_key(&blob_id) {
                    continue;
                }

                grouped
                    .entry(blob_id.clone())
                    .and_modify(|existing| {
                        if path < *existing {
                            *existing = path.clone();
                        }
                    })
                    .or_insert(path);
            }

            let mut records: Vec<RedbTransferRecord> = grouped
                .into_iter()
                .map(|(blob_id, path)| RedbTransferRecord {
                    transfer_id: transfer_id as i64,
                    blob_size: remote_map.get(&blob_id).copied().unwrap_or(0),
                    path,
                    blob_id,
                })
                .collect();

            if records.is_empty() {
                return;
            }

            records.sort_by(|a, b| a.path.cmp(&b.path).then_with(|| a.blob_id.cmp(&b.blob_id)));

            let mut items: Vec<FileTransferItem> = Vec::with_capacity(records.len());
            for record in &records {
                items.push(FileTransferItem {
                    transfer_id,
                    blob_id: record.blob_id.clone(),
                    blob_size: record.blob_size,
                    path: record.path.clone(),
                });
            }

            redb_history.store_transfers(records).await?;

            for item in items {
                yield item;
            }
        };

        stream
            .boxed()
            .track("Database::populate_missing_files_for_transfer")
            .boxed()
    }

    pub(crate) async fn select_files_transfer(
        &self,
        input_stream: impl Stream<Item = CopiedTransferItem> + Unpin + Send + 'static,
    ) -> DBOutputStream<'static, FileTransferItem> {
        let redb_history = self.redb_history.clone();
        input_stream
            .bounded_wait_chunks(self.max_variable_number / 2, TIMEOUT)
            .then(move |chunk: Vec<CopiedTransferItem>| {
                let redb_history = redb_history.clone();
                Box::pin(async move {
                    if chunk.is_empty() {
                        // SQL is otherwise not valid
                        return stream::iter(vec![]);
                    }

                    let keys: Vec<(i64, String)> = chunk
                        .into_iter()
                        .map(|row| (row.transfer_id as i64, row.path))
                        .collect();

                    stream::iter(match redb_history.lookup_transfers(&keys).await {
                        Ok(records) => records
                            .into_iter()
                            .map(|record| {
                                Ok(FileTransferItem {
                                    transfer_id: record.transfer_id as u32,
                                    blob_id: record.blob_id,
                                    blob_size: record.blob_size,
                                    path: record.path,
                                })
                            })
                            .collect::<Vec<_>>(),
                        Err(e) => vec![Err(e)],
                    })
                })
            })
            .flatten()
            .boxed()
            .track("Database::select_files_transfer")
            .boxed()
    }

    pub async fn move_files<'a>(
        &self,
        src_raw: String,
        dst_raw: String,
        mv_type_hint: PathType,
        now: DateTime<Utc>,
    ) -> DBOutputStream<'a, MoveEvent> {
        let src_norm = trim_trailing_slash(normalize_path(src_raw.to_string()));
        let dst_norm = normalize_path(dst_raw.to_string());
        let redb_history = self.redb_history.clone();

        let s = try_stream! {
            let mut entries: Vec<(String, String, String)> = Vec::new();
            let mut violations: Vec<MoveViolation> = Vec::new();

            let is_dir = match mv_type_hint {
                PathType::Dir => true,
                PathType::File => false,
                PathType::Unknown => {
                    redb_history
                        .latest_file_for_path(&src_norm)
                        .await?
                        .is_none()
                }
            };

            if is_dir {
                let prefix = format!("{}/", src_norm);
                let records = redb_history
                    .latest_filesystem_records_with_prefix(&prefix)
                    .await?;
                for record in records {
                    if let Some(blob_id) = record.blob_id {
                        let suffix = record.path[prefix.len()..].to_string();
                        let dst_path = format!("{}/{}", dst_norm, suffix);
                        entries.push((record.path, dst_path, blob_id));
                    }
                }
            } else if let Some(record) = redb_history.latest_file_for_path(&src_norm).await? {
                if let Some(blob_id) = record.blob_id {
                    entries.push((record.path, dst_norm.clone(), blob_id));
                }
            }

            if entries.is_empty() {
                violations.push(MoveViolation {
                    code: MoveViolationCode::SourceNotFound,
                    detail: String::new(),
                });
            }

            if violations.is_empty() {
                let mut unique_dests = Vec::new();
                let mut seen = HashSet::new();
                for (_, dst_path, _) in &entries {
                    if seen.insert(dst_path.clone()) {
                        unique_dests.push(dst_path.clone());
                    }
                }

                let existing_destinations = redb_history
                    .latest_filesystem_records_for_paths(&unique_dests)
                    .await?;

                for dest in unique_dests {
                    if existing_destinations.contains_key(&dest) {
                        violations.push(MoveViolation {
                            code: MoveViolationCode::DestinationExistsDb,
                            detail: dest,
                        });
                    }
                }
            }

            if violations.is_empty() && entries.iter().any(|(src, dst, _)| src == dst) {
                violations.push(MoveViolation {
                    code: MoveViolationCode::SourceEqualsDestination,
                    detail: String::new(),
                });
            }

            if !violations.is_empty() {
                for violation in violations {
                    yield MoveEvent::Violation(violation);
                }
                return;
            }

            let mut redb_records = Vec::with_capacity(entries.len() * 2);
            for (src_path, dst_path, blob_id) in &entries {
                redb_records.push(RedbFileRecord {
                    id: 0,
                    uuid: Uuid::new_v4().to_string(),
                    path: src_path.clone(),
                    blob_id: None,
                    valid_from: now,
                });
                redb_records.push(RedbFileRecord {
                    id: 0,
                    uuid: Uuid::new_v4().to_string(),
                    path: dst_path.clone(),
                    blob_id: Some(blob_id.clone()),
                    valid_from: now,
                });
            }

            redb_history.append_files(redb_records).await?;

            entries.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            for (src_path, dst_path, blob_id) in entries {
                yield MoveEvent::Instruction(MoveInstr {
                    src_path,
                    dst_path,
                    blob_id,
                });
            }
        };

        s.boxed().track("move_files").boxed()
    }

    pub async fn remove_files<'a>(
        &self,
        paths_with_hint: Vec<(String, PathType)>,
        now: DateTime<Utc>,
    ) -> DBOutputStream<'a, RmEvent> {
        let norm_paths_with_hint: Vec<_> = paths_with_hint
            .iter()
            .map(|(p, h)| {
                (
                    trim_trailing_slash(normalize_path(p.to_string())),
                    h.clone(),
                )
            })
            .collect();
        let redb_history = self.redb_history.clone();

        let s = try_stream! {
            let mut instructions: BTreeMap<String, String> = BTreeMap::new();
            let mut violations: Vec<RmViolation> = Vec::new();

            for (path, hint) in norm_paths_with_hint {
                let is_dir = match hint {
                    PathType::Dir => true,
                    PathType::File => false,
                    PathType::Unknown => {
                        redb_history
                            .latest_file_for_path(&path)
                            .await?
                            .is_none()
                    }
                };

                let mut matches: Vec<(String, String)> = Vec::new();
                if is_dir {
                    let prefix = format!("{}/", path);
                    let records = redb_history
                        .latest_filesystem_records_with_prefix(&prefix)
                        .await?;
                    for record in records {
                        if let Some(blob_id) = record.blob_id {
                            matches.push((record.path, blob_id));
                        }
                    }
                } else if let Some(record) = redb_history.latest_file_for_path(&path).await? {
                    if let Some(blob_id) = record.blob_id {
                        matches.push((record.path, blob_id));
                    }
                }

                if matches.is_empty() {
                    violations.push(RmViolation {
                        code: RmViolationCode::SourceNotFound,
                        detail: path,
                    });
                    continue;
                }

                for (matched_path, blob_id) in matches {
                    instructions.insert(matched_path, blob_id);
                }
            }

            if !violations.is_empty() {
                for violation in violations {
                    yield RmEvent::Violation(violation);
                }
                return;
            }

            if instructions.is_empty() {
                return;
            }

            let mut redb_records = Vec::with_capacity(instructions.len());
            for path in instructions.keys() {
                redb_records.push(RedbFileRecord {
                    id: 0,
                    uuid: Uuid::new_v4().to_string(),
                    path: path.clone(),
                    blob_id: None,
                    valid_from: now,
                });
            }

            redb_history.append_files(redb_records).await?;

            for (path, blob_id) in instructions {
                yield RmEvent::Instruction(RmInstr {
                    path,
                    target_blob_id: blob_id,
                });
            }
        };

        s.boxed().track("remove_files").boxed()
    }
}

fn blob_storage_path(blob_id: &str) -> String {
    if blob_id.len() > 6 {
        format!("{}/{}/{}", &blob_id[0..2], &blob_id[2..4], &blob_id[4..])
    } else {
        blob_id.to_string()
    }
}

fn path_matches(path: &str, selectors: &[String]) -> bool {
    if selectors.is_empty() {
        return true;
    }

    selectors.iter().any(|prefix| path.starts_with(prefix))
}

#[inline]
fn normalize_path(mut p: String) -> String {
    while p.ends_with('/') && p.len() > 1 {
        p.pop();
    }
    p
}
#[inline]
fn trim_trailing_slash(mut s: String) -> String {
    while s.ends_with('/') && s.len() > 1 {
        s.pop();
    }
    s
}
