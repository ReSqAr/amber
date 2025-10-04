use crate::db::cleaner::Cleaner;
use crate::db::error::DBError;
use crate::db::models::{
    AvailableBlob, Blob, BlobAssociatedToFiles, BlobTransferItem, Connection, CopiedTransferItem,
    CurrentRepository, File, FileTransferItem, InsertBlob, InsertFile, InsertMaterialisation,
    InsertRepositoryName, MaterialisationProjection, MissingFile, MoveEvent, MoveInstr,
    MoveViolation, Observation, ObservedBlob, PathType, Repository, RepositoryName, RmEvent,
    RmInstr, RmViolation, RmViolationCode, VirtualFile,
};
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
use uuid::Uuid;

#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
    max_variable_number: usize,
    cleaner: Cleaner,
    virtual_filesystem: VirtualFilesystemStore,
}

const TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_millis(5);

pub(crate) type DBOutputStream<'a, T> = BoxStream<'a, Result<T, DBError>>;

impl Database {
    pub fn new(pool: SqlitePool, virtual_filesystem: VirtualFilesystemStore) -> Self {
        let cleaner = Cleaner::new(pool.clone());
        Self {
            pool,
            max_variable_number: 32000, // 32766 - actually: https://sqlite.org/limits.html
            cleaner,
            virtual_filesystem,
        }
    }

    pub async fn clean(&self) -> Result<(), DBError> {
        sqlx::query("DELETE FROM transfers;")
            .execute(&self.pool)
            .await
            .inspect_err(|e| log::error!("Database::clean failed: {e}"))?;

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

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT INTO files (uuid, path, blob_id, valid_from) VALUES {}",
                placeholders
            );

            let mut query = sqlx::query(&query_str);

            for file in &chunk {
                let uuid = Uuid::new_v4().to_string();
                query = query
                    .bind(uuid)
                    .bind(&file.path)
                    .bind(&file.blob_id)
                    .bind(file.valid_from);
            }

            let result = query
                .execute(&self.pool)
                .await
                .inspect_err(|e| log::error!("Database::add_files failed: {e}"))?;
            total_inserted += result.rows_affected();
            total_attempted += chunk.len() as u64;
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

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?, ?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT INTO blobs (uuid, repo_id, blob_id, blob_size, has_blob, path, valid_from) VALUES {}",
                placeholders
            );

            let mut query = sqlx::query(&query_str);

            for blob in &chunk {
                let uuid = Uuid::new_v4().to_string();
                query = query
                    .bind(uuid)
                    .bind(&blob.repo_id)
                    .bind(&blob.blob_id)
                    .bind(blob.blob_size)
                    .bind(blob.has_blob)
                    .bind(&blob.path)
                    .bind(blob.valid_from);
            }

            let result = query
                .execute(&self.pool)
                .await
                .inspect_err(|e| log::error!("Database::add_blobs failed: {e}"))?;
            total_inserted += result.rows_affected();
            total_attempted += chunk.len() as u64;
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

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT INTO blobs (uuid, repo_id, blob_id, blob_size, has_blob, path, valid_from)
                    WITH VS(uuid, repo_id, has_blob, path, valid_from) AS (
                        VALUES {}
                    )
                    SELECT
                        uuid,
                        repo_id,
                        blob_id,
                        blob_size,
                        has_blob,
                        path,
                        valid_from
                    FROM VS
                    INNER JOIN latest_available_blobs USING (repo_id, path)",
                placeholders
            );

            let mut query = sqlx::query(&query_str);

            for blob in &chunk {
                let uuid = Uuid::new_v4().to_string();
                query = query
                    .bind(uuid)
                    .bind(&blob.repo_id)
                    .bind(blob.has_blob)
                    .bind(&blob.path)
                    .bind(blob.valid_from);
            }

            let result = query
                .execute(&self.pool)
                .await
                .inspect_err(|e| log::error!("Database::observe_blobs failed: {e}"))?;
            total_inserted += result.rows_affected();
            total_attempted += chunk.len() as u64;
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
        self.stream(
            "Database::select_files",
            query(
                "
            SELECT uuid, path, blob_id, valid_from
            FROM files
            WHERE id > ?",
            )
            .bind(last_index),
        )
    }

    pub async fn select_blobs(&self, last_index: i32) -> DBOutputStream<'static, Blob> {
        self.stream(
            "Database::select_blobs",
            query(
                "
                SELECT uuid, repo_id, blob_id, blob_size, has_blob, path, valid_from
                FROM blobs
                WHERE id > ?",
            )
            .bind(last_index),
        )
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

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT OR IGNORE INTO files (uuid, path, blob_id, valid_from) VALUES {}",
                placeholders
            );

            let mut query = sqlx::query(&query_str);

            for file in &chunk {
                query = query
                    .bind(&file.uuid)
                    .bind(&file.path)
                    .bind(&file.blob_id)
                    .bind(file.valid_from)
            }

            let result = query
                .execute(&self.pool)
                .await
                .inspect_err(|e| log::error!("Database::merge_files failed: {e}"))?;
            total_inserted += result.rows_affected();
            total_attempted += chunk.len() as u64;
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

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?, ?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT OR IGNORE INTO blobs (uuid, repo_id, blob_id, blob_size, has_blob, path, valid_from) VALUES {}",
                placeholders
            );

            let mut query = sqlx::query(&query_str);

            for file in &chunk {
                query = query
                    .bind(&file.uuid)
                    .bind(&file.repo_id)
                    .bind(&file.blob_id)
                    .bind(file.blob_size)
                    .bind(file.has_blob)
                    .bind(&file.path)
                    .bind(file.valid_from)
            }

            let result = query
                .execute(&self.pool)
                .await
                .inspect_err(|e| log::error!("Database::merge_blobs failed: {e}"))?;
            total_inserted += result.rows_affected();
            total_attempted += chunk.len() as u64;
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
        sqlx::query_as::<_, Repository>(
            "
            INSERT INTO repositories (repo_id, last_file_index, last_blob_index, last_name_index)
            SELECT
                (SELECT repo_id FROM current_repository LIMIT 1),
                (SELECT COALESCE(MAX(id), -1) FROM files),
                (SELECT COALESCE(MAX(id), -1) FROM blobs),
                (SELECT COALESCE(MAX(id), -1) FROM repository_names)
            ON CONFLICT(repo_id) DO UPDATE
            SET
                last_file_index = MAX(excluded.last_file_index, repositories.last_file_index),
                last_blob_index = MAX(excluded.last_blob_index, repositories.last_blob_index),
                last_name_index = MAX(excluded.last_name_index, repositories.last_name_index)
            RETURNING repo_id, last_file_index, last_blob_index, last_name_index;
            ",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(DBError::from)
    }

    pub(crate) fn available_blobs(
        &self,
        repo_id: String,
    ) -> impl Stream<Item = Result<AvailableBlob, DBError>> + Unpin + Send + Sized + 'static {
        self.stream(
            "Database::available_blobs",
            query(
                "
                SELECT repo_id, blob_id, blob_size, path
                FROM latest_available_blobs
                WHERE repo_id = ?",
            )
            .bind(repo_id),
        )
    }

    pub(crate) fn missing_blobs(
        &self,
        repo_id: String,
    ) -> impl Stream<Item = Result<BlobAssociatedToFiles, DBError>> + Unpin + Send + Sized + 'static
    {
        self.stream(
            "Database::missing_blobs",
            query(
                "
                WITH blobs_with_repository_names AS (
                    SELECT
                        blob_id,
                        json_group_array(COALESCE(rn.name, ab.repo_id)) AS repository_names,
                        MAX(CASE WHEN ab.repo_id = ? THEN 1 ELSE 0 END) AS is_available
                    FROM latest_available_blobs ab
                        LEFT JOIN latest_repository_names rn ON ab.repo_id = rn.repo_id
                    GROUP BY
                        blob_id
                ), missing_blobs_with_paths AS (
                    SELECT
                        fs.blob_id,
                        brn.repository_names,
                        json_group_array(fs.path) AS paths
                    FROM latest_filesystem_files fs
                        LEFT JOIN blobs_with_repository_names brn ON fs.blob_id = brn.blob_id
                    WHERE NOT COALESCE(brn.is_available, FALSE)
                    GROUP BY fs.blob_id, brn.repository_names
                )
                SELECT
                    blob_id,
                    paths,
                    COALESCE(repository_names, '[]') AS repositories_with_blob
                FROM missing_blobs_with_paths
                ORDER BY paths;
            ",
            )
            .bind(repo_id),
        )
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

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT INTO materialisations (path, blob_id, valid_from) VALUES {}",
                placeholders
            );

            let mut query = sqlx::query(&query_str);

            for mat in &chunk {
                query = query
                    .bind(&mat.path)
                    .bind(&mat.blob_id)
                    .bind(mat.valid_from);
            }

            let result = query
                .execute(&self.pool)
                .await
                .inspect_err(|e| log::error!("Database::add_materialisations failed: {e}"))?;
            total_inserted += result.rows_affected();
            total_attempted += chunk.len() as u64;
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
        let query = r#"
            WITH
                locally_available_blobs AS (
                    SELECT
                        blob_id,
                        blob_size
                    FROM latest_available_blobs
                        INNER JOIN current_repository USING (repo_id)
                ),
                latest_filesystem_files_with_materialisation_and_availability AS (
                    SELECT
                        path,
                        m.blob_id as materialisation_last_blob_id,
                        CASE
                            WHEN a.blob_id IS NOT NULL THEN TRUE
                            ELSE FALSE
                        END AS local_has_blob,
                        f.blob_id,
                        blob_size
                    FROM latest_filesystem_files f
                        LEFT JOIN locally_available_blobs a USING (blob_id)
                        LEFT JOIN latest_materialisations m USING (path)
                    UNION ALL
                    SELECT -- files which have are supposed to be deleted but still have a materialisation
                           path,
                           m.blob_id as materialisation_last_blob_id,
                           FALSE AS local_has_blob,
                           NULL AS blob_id,
                           NULL AS blob_size
                    FROM latest_materialisations m
                        LEFT JOIN latest_filesystem_files f USING (path)
                    WHERE f.blob_id IS NULL
                )
            SELECT
                path,
                materialisation_last_blob_id,
                blob_id as target_blob_id,
                blob_size as target_blob_size,
                local_has_blob as local_has_target_blob
            FROM latest_filesystem_files_with_materialisation_and_availability;
    "#;

        let pool = self.pool.clone();
        let s = try_stream! {
            let mut rows = sqlx::query_as::<_, (String, Option<String>, Option<String>, Option<i64>, bool)>(query)
                .fetch(&pool);

            while let Some(row) = rows.next().await {
                let (path, materialisation_last_blob_id, target_blob_id, target_blob_size, local_has_target_blob) =
                    row.map_err(DBError::from)?;
                yield MaterialisationProjection {
                    path,
                    materialisation_last_blob_id,
                    target_blob_id,
                    target_blob_size,
                    local_has_target_blob,
                };
            }
        };

        let batch_id: i64 = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        self.virtual_filesystem
            .update_materialisation_data(s.boxed(), batch_id)
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
        self.stream("Database::populate_missing_blobs_for_transfer",
                    query(
                        "
            INSERT INTO transfers (transfer_id, blob_id, blob_size, path)
            WITH
                local_blobs AS (
                    SELECT blob_id
                    FROM latest_available_blobs
                    INNER JOIN current_repository USING (repo_id)
                ),
                remote_blobs AS (
                    SELECT
                        blob_id,
                        blob_size
                    FROM latest_available_blobs
                    WHERE repo_id = $1
                ),
                path_selectors AS (
                    SELECT value AS path_selector
                    FROM json_each($2)
                ),
                selected_files AS (
                    SELECT f.blob_id, f.path
                    FROM latest_filesystem_files f
                    INNER JOIN path_selectors p ON f.path LIKE p.path_selector || '%'
                    UNION ALL
                    SELECT f.blob_id, f.path
                    FROM latest_filesystem_files f
                    WHERE $2 = '[]'
                ),
                missing_blob_ids AS (
                    SELECT DISTINCT f.blob_id
                    FROM selected_files f
                    LEFT JOIN local_blobs lb ON f.blob_id = lb.blob_id
                    WHERE lb.blob_id IS NULL
                )
            SELECT
                $3 AS transfer_id,
                m.blob_id,
                rb.blob_size,
                CASE
                    WHEN length(m.blob_id) > 6
                        THEN substr(m.blob_id, 1, 2) || '/' || substr(m.blob_id, 3, 2) || '/' || substr(m.blob_id, 5)
                    ELSE  m.blob_id
                END AS path
            FROM missing_blob_ids m
            INNER JOIN remote_blobs rb ON m.blob_id = rb.blob_id
            RETURNING transfer_id, blob_id, path;",
                    )
                        .bind(remote_repo_id)
                        .bind(serde_json::to_string(&paths).unwrap())
                        .bind(transfer_id),
        )
    }

    pub(crate) async fn select_blobs_transfer(
        &self,
        input_stream: impl Stream<Item = CopiedTransferItem> + Unpin + Send + 'static,
    ) -> DBOutputStream<'static, BlobTransferItem> {
        let pool = self.pool.clone();
        input_stream.bounded_wait_chunks(self.max_variable_number / 2, TIMEOUT).then(move |chunk: Vec<CopiedTransferItem>| {
            let pool = pool.clone();
            Box::pin(async move {
                if chunk.is_empty() { // SQL is otherwise not valid
                    return stream::iter(vec![]);
                }

                let placeholders = chunk
                    .iter()
                    .map(|_| "(?, ?)")
                    .collect::<Vec<_>>()
                    .join(", ");

                let query_str = format!("
                    WITH DATA(transfer_id, path) AS (VALUES {})
                    SELECT transfer_id, blob_id, path FROM transfers INNER JOIN DATA using (transfer_id, path)
                    ;
                ", placeholders);

                let mut query = sqlx::query_as::<_, BlobTransferItem>(&query_str);
                for row in chunk {
                    query = query
                        .bind(row.transfer_id)
                        .bind(row.path);
                }

                stream::iter(match query.fetch_all(&pool).await {
                    Ok(v) => v.into_iter().map(Ok).collect::<Vec<Result<BlobTransferItem, DBError>>>(),
                    Err(e) => vec!(Err(DBError::from(e))),
                })
            })
        }).flatten().boxed().track("Database::select_blobs_transfer").boxed()
    }

    pub(crate) async fn populate_missing_files_for_transfer(
        &self,
        transfer_id: u32,
        local_repo_id: String,
        remote_repo_id: String,
        paths: Vec<String>,
    ) -> DBOutputStream<'static, FileTransferItem> {
        self.stream(
            "Database::populate_missing_files_for_transfer",
            query(
                "
            INSERT INTO transfers (transfer_id, blob_id, blob_size, path)
            WITH
                local_blobs AS (
                    SELECT blob_id
                    FROM latest_available_blobs
                    WHERE repo_id = $1
                ),
                remote_blobs AS (
                    SELECT
                        blob_id,
                        blob_size
                    FROM latest_available_blobs
                    WHERE repo_id = $2
                ),
                path_selectors AS (
                    SELECT value AS path_selector
                    FROM json_each($3)
                ),
                selected_files AS (
                    SELECT f.blob_id, f.path
                    FROM latest_filesystem_files f
                    INNER JOIN path_selectors p ON f.path LIKE p.path_selector || '%'
                    UNION ALL
                    SELECT f.blob_id, f.path
                    FROM latest_filesystem_files f
                    WHERE $3 = '[]'
                ),
                missing_file_blob_ids AS (
                    SELECT
                        f.blob_id,
                        MIN(f.path) as path
                    FROM selected_files f
                    LEFT JOIN local_blobs lb ON f.blob_id = lb.blob_id
                    WHERE lb.blob_id IS NULL
                    GROUP BY f.blob_id
                )
            SELECT
                $4 AS transfer_id,
                m.blob_id,
                rb.blob_size,
                m.path
            FROM missing_file_blob_ids m
            INNER JOIN remote_blobs rb ON m.blob_id = rb.blob_id
            RETURNING transfer_id, blob_id, blob_size, path;",
            )
            .bind(local_repo_id)
            .bind(remote_repo_id)
            .bind(serde_json::to_string(&paths).unwrap())
            .bind(transfer_id),
        )
    }

    pub(crate) async fn select_files_transfer(
        &self,
        input_stream: impl Stream<Item = CopiedTransferItem> + Unpin + Send + 'static,
    ) -> DBOutputStream<'static, FileTransferItem> {
        let pool = self.pool.clone();
        input_stream.bounded_wait_chunks(self.max_variable_number / 2, TIMEOUT).then(move |chunk: Vec<CopiedTransferItem>| {
            let pool = pool.clone();
            Box::pin(async move {
                if chunk.is_empty() { // SQL is otherwise not valid
                    return stream::iter(vec![]);
                }

                let placeholders = chunk
                    .iter()
                    .map(|_| "(?, ?)")
                    .collect::<Vec<_>>()
                    .join(", ");

                let query_str = format!("
                    WITH DATA(transfer_id, path) AS (VALUES {})
                    SELECT transfer_id, blob_id, blob_size, path FROM transfers INNER JOIN DATA using (transfer_id, path)
                    ;
                ", placeholders);

                let mut query = sqlx::query_as::<_, FileTransferItem>(&query_str);
                for row in chunk {
                    query = query
                        .bind(row.transfer_id)
                        .bind(row.path);
                }

                stream::iter(match query.fetch_all(&pool).await {
                    Ok(v) => v.into_iter().map(Ok).collect::<Vec<Result<FileTransferItem, DBError>>>(),
                    Err(e) => vec!(Err(DBError::from(e))),
                })
            })
        }).flatten().boxed().track("Database::select_files_transfer").boxed()
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
        let now_str = now.to_rfc3339();

        let pool = self.pool.clone();

        let s = try_stream! {
            // 1) TEMP TABLE
            sqlx::query("DROP TABLE IF EXISTS temp_mv_map;")
                .execute(&pool).await.inspect_err(|e| log::error!("db::move_files: 1) drop table: {e}")).map_err(DBError::from)?;
            sqlx::query("CREATE TABLE temp_mv_map (src_path TEXT PRIMARY KEY, dst_path TEXT NOT NULL, blob_id TEXT NOT NULL);")
                .execute(&pool).await.inspect_err(|e| log::error!("db::move_files: 1) create table: {e}")).map_err(DBError::from)?;

            // 2) POPULATE (named params for readability; bind in order of appearance)
            sqlx::query(r#"
INSERT INTO temp_mv_map (src_path, dst_path, blob_id)
WITH is_dir(v) AS (
  SELECT
    CASE
      WHEN $1 = 'dir'  THEN 1
      WHEN $1 = 'file' THEN 0
      WHEN EXISTS (SELECT 1 FROM latest_filesystem_files WHERE path = $2) THEN 0
      ELSE 1
    END
)
SELECT
  f.path AS src_path,
  CASE
    WHEN (SELECT v FROM is_dir)=1
      THEN $3 || '/' || substr(f.path, length($2) + 2)
    ELSE $3
  END AS dst_path,
  f.blob_id
FROM latest_filesystem_files AS f, is_dir
WHERE
  ((SELECT v FROM is_dir)=1 AND f.path LIKE $2 || '/%')
  OR
  ((SELECT v FROM is_dir)=0 AND f.path = $2);
"#)
                .bind(mv_type_hint)
                .bind(&src_norm)
                .bind(&dst_norm)
                .execute(&pool).await.inspect_err(|e| log::error!("db::move_files: 2) insert data: {e}")).map_err(DBError::from)?;

            // 3) VIOLATIONS — stream them back (as data). If any → STOP (no DB mutation).
            let mut vio_rows = pool.fetch_many(sqlx::query_as::<_, MoveViolation>(r#"
  SELECT 'source_not_found' AS code, '' AS detail
  WHERE NOT EXISTS (SELECT 1 FROM temp_mv_map)
UNION ALL
  SELECT 'destination_exists_db' AS code, m.dst_path AS detail
  FROM temp_mv_map AS m
  JOIN latest_filesystem_files AS d ON d.path = m.dst_path
UNION ALL
  SELECT 'source_equals_destination' AS code, '' AS detail
  WHERE EXISTS (SELECT 1 FROM temp_mv_map WHERE src_path = dst_path)
;"#));

            let mut had_violation = false;
            while let Some(item) = vio_rows.next().await {
                match item {
                    Ok(Either::Right(v)) => {
                        had_violation = true;
                        yield MoveEvent::Violation(MoveViolation::from_row(&v)?);
                    }
                    Ok(Either::Left(_)) => continue,
                    Err(e) => Err(DBError::from(e)).inspect_err(|e| log::error!("db::move_files: 3) load violations: {e}"))?,
                }
            }
            if had_violation {
                // stop streaming here; no DB mutation when violations exist
                return;
            }

            // 4) INSERT (tombstones + new rows) — single statement
            sqlx::query(r#"
INSERT INTO files (uuid, path, blob_id, valid_from)
SELECT
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(6))),
    src_path,
    NULL,
    $1
FROM temp_mv_map
UNION ALL
SELECT
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(6))),
    dst_path,
    blob_id,
    $1
FROM temp_mv_map;
"#)
                .bind(&now_str)
                .execute(&pool).await.inspect_err(|e| log::error!("db::move_files: 4) insert files: {e}")).map_err(DBError::from)?;

            // 5) STREAM INSTRUCTIONS (as MoveEvent::Instruction)
            let mut instr_rows = pool.fetch_many(sqlx::query_as::<_, MoveInstr>(
                "SELECT src_path, dst_path, blob_id FROM temp_mv_map;"
            ));
            while let Some(item) = instr_rows.next().await {
                match item {
                    Ok(Either::Right(instr)) => yield MoveEvent::Instruction(MoveInstr::from_row(&instr)?),
                    Ok(Either::Left(_)) => continue,
                    Err(e) => Err(DBError::from(e)).inspect_err(|e| log::error!("db::move_files: 5) stream instructions: {e}"))?,
                }
            }

            // 6) CLEANUP
            sqlx::query("DROP TABLE IF EXISTS temp_mv_map;")
                .execute(&pool).await.map_err(DBError::from).inspect_err(|e| log::error!("db::move_files: 6) cleanup: {e}"))?;
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
        let now_str = now.to_rfc3339();

        let pool = self.pool.clone();
        let s = try_stream! {
            // 1) TEMP TABLE
            sqlx::query("DROP TABLE IF EXISTS temp_rm_set;")
                .execute(&pool).await.inspect_err(|e| log::error!("db::remove_files: 1) drop table: {e}")).map_err(DBError::from)?;
            sqlx::query(
                "CREATE TABLE temp_rm_set (
                    path TEXT PRIMARY KEY,
                    target_blob_id STRING NOT NULL
                );",
            )
            .execute(&pool)
            .await
            .inspect_err(|e| log::error!("db::remove_files: 1) create table: {e}"))
            .map_err(DBError::from)?;

            // 2) POPULATE
            let mut violations = Vec::new();
            for (path, hint) in norm_paths_with_hint {
                let result = sqlx::query(r#"
INSERT OR REPLACE INTO temp_rm_set (path, target_blob_id)
WITH is_dir(v) AS (
  SELECT
    CASE
      WHEN $1 = 'dir'  THEN 1
      WHEN $1 = 'file' THEN 0
      WHEN EXISTS (SELECT 1 FROM latest_filesystem_files WHERE path = $2) THEN 0
      ELSE 1
    END
)
SELECT
  f.path,
  f.blob_id as target_blob_id
FROM latest_filesystem_files AS f, is_dir
WHERE
  ((SELECT v FROM is_dir)=1 AND f.path LIKE $2 || '/%')
  OR
  ((SELECT v FROM is_dir)=0 AND f.path = $2);
"#)
                .bind(hint)
                .bind(&path)
                .execute(&pool).await.inspect_err(|e| log::error!("db::move_files: 2) insert data: {e}")).map_err(DBError::from)?;
                if result.rows_affected() == 0 {
                    violations.push(RmEvent::Violation(RmViolation{code: RmViolationCode::SourceNotFound, detail: path}))
                }
            }


            let had_violation = !violations.is_empty();
            for violation in violations.drain(0..violations.len())  {
                yield violation;
            }
            if had_violation {
                // stop streaming here; no DB mutation when violations exist
                return;
            }

            // 3) INSERT tombstones (append-only)
            sqlx::query(r#"
INSERT INTO files (uuid, path, blob_id, valid_from)
SELECT lower(hex(randomblob(16))), path, NULL, $1
FROM temp_rm_set;
"#)
                .bind(&now_str)
                .execute(&pool).await.map_err(DBError::from)?;

            // 4) STREAM instructions (path, target_blob_id)
            let mut instr_rows = pool.fetch_many(sqlx::query_as::<_, RmInstr>(
                "SELECT path, target_blob_id FROM temp_rm_set;"
            ));
            while let Some(item) = instr_rows.next().await {
                match item {
                    Ok(Either::Right(instr)) => yield RmEvent::Instruction(RmInstr::from_row(&instr)?),
                    Ok(Either::Left(_))      => continue,
                    Err(e) => Err(DBError::from(e)).inspect_err(|e| log::error!("db::remove_files: 4) stream instructions: {e}"))?,
                }
            }

            // 5) CLEANUP
            sqlx::query("DROP TABLE IF EXISTS temp_rm_set;")
                .execute(&pool).await.map_err(DBError::from).inspect_err(|e| log::error!("db::remove_files: 5) cleanup: {e}"))?;

        };

        s.boxed().track("remove_files").boxed()
    }
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
