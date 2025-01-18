use crate::db::models::{
    Blob, BlobObjectId, CurrentRepository, File, FilePathWithObjectId, InputBlob, InputFile,
    Repository,
};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use log::debug;
use sqlx::query::Query;
use sqlx::sqlite::SqliteArguments;
use sqlx::{query, Either, Executor, FromRow, Sqlite, SqlitePool};
use uuid::Uuid;

#[derive(Clone)]
pub struct DB {
    pool: SqlitePool,
    chunk_size: usize,
}

type DBOutputStream<'a, T> = BoxStream<'a, Result<T, sqlx::Error>>;

impl DB {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            chunk_size: 100,
        }
    }

    fn stream<'a, T>(&self, q: Query<'a, Sqlite, SqliteArguments<'a>>) -> DBOutputStream<'a, T>
    where
        T: Send + Unpin + for<'r> FromRow<'r, <Sqlite as sqlx::Database>::Row> + 'a,
    {
        // inlined from:
        //   sqlx-core-0.8.3/src/query_as.rs
        // due to lifetime issues with .fetch
        self.pool
            .clone()
            .fetch_many(q)
            .map(|v| match v {
                Ok(Either::Right(row)) => T::from_row(&row).map(Either::Right),
                Ok(Either::Left(v)) => Ok(Either::Left(v)),
                Err(e) => Err(e),
            })
            .try_filter_map(|step| async move { Ok(step.right()) })
            .boxed()
    }

    pub async fn setup_db(&self) -> Result<(), sqlx::Error> {
        sqlx::query("PRAGMA journal_mode = WAL;")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_or_create_current_repository(&self) -> Result<CurrentRepository, sqlx::Error> {
        let potential_new_repository_id = Uuid::new_v4().to_string();
        if let Some(repo) = sqlx::query_as::<_, CurrentRepository>(
            "INSERT OR IGNORE INTO current_repository (id, repo_id) VALUES (1, ?)
            RETURNING repo_id;",
        )
        .bind(potential_new_repository_id)
        .fetch_optional(&self.pool)
        .await?
        {
            return Ok(repo);
        }
        sqlx::query_as::<_, CurrentRepository>("SELECT id, repo_id FROM current_repository LIMIT 1")
            .fetch_one(&self.pool)
            .await
    }

    pub async fn add_file<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = InputFile> + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s.chunks(self.chunk_size);

        while let Some(chunk) = chunk_stream.next().await {
            if chunk.is_empty() {
                continue;
            }

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT INTO files (uuid, path, object_id, valid_from) VALUES {}",
                placeholders
            );

            let mut query = sqlx::query(&query_str);

            for file in &chunk {
                let uuid = Uuid::new_v4().to_string();
                query = query
                    .bind(uuid)
                    .bind(&file.path)
                    .bind(&file.object_id)
                    .bind(&file.valid_from);
            }

            let result = query.execute(&self.pool).await?;
            total_inserted += result.rows_affected();
            total_attempted += chunk.len() as u64;
        }

        debug!(
            "files added: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(())
    }

    pub async fn add_blob<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = InputBlob> + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s.chunks(self.chunk_size);

        while let Some(chunk) = chunk_stream.next().await {
            if chunk.is_empty() {
                continue;
            }

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT INTO blobs (uuid, repo_id, object_id, has_blob, valid_from) VALUES {}",
                placeholders
            );

            let mut query = sqlx::query(&query_str);

            for blob in &chunk {
                let uuid = Uuid::new_v4().to_string();
                query = query
                    .bind(uuid)
                    .bind(&blob.repo_id)
                    .bind(&blob.object_id)
                    .bind(&blob.has_blob)
                    .bind(&blob.valid_from);
            }

            let result = query.execute(&self.pool).await?;
            total_inserted += result.rows_affected();
            total_attempted += chunk.len() as u64;
        }

        debug!(
            "blobs added: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(())
    }

    pub fn select_repositories(&self) -> DBOutputStream<'static, Repository> {
        self.stream(query(
            "SELECT repo_id, last_file_index, last_blob_index FROM repositories",
        ))
    }

    pub fn select_files(&self, last_index: i32) -> DBOutputStream<'static, File> {
        self.stream(
            query(
                "
            SELECT uuid, path, object_id, valid_from
            FROM files
            WHERE id > ?",
            )
            .bind(last_index),
        )
    }

    pub fn select_blobs(&self, last_index: i32) -> DBOutputStream<'static, Blob> {
        self.stream(
            query(
                "
                SELECT uuid, repo_id, object_id, has_blob, valid_from
                FROM blobs
                WHERE id > ?",
            )
            .bind(last_index),
        )
    }

    pub async fn merge_repositories<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = Repository> + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s.chunks(self.chunk_size);

        while let Some(chunk) = chunk_stream.next().await {
            if chunk.is_empty() {
                continue;
            }

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT INTO repositories (repo_id, last_file_index, last_blob_index)
                 VALUES {}
                 ON CONFLICT(repo_id) DO UPDATE SET
                    last_file_index = max(last_file_index, excluded.last_file_index),
                    last_blob_index = max(last_blob_index, excluded.last_blob_index)
                ",
                placeholders
            );

            let mut query = sqlx::query(&query_str);

            for repo in &chunk {
                query = query
                    .bind(&repo.repo_id)
                    .bind(&repo.last_file_index)
                    .bind(&repo.last_blob_index)
            }

            let result = query.execute(&self.pool).await?;
            total_inserted += result.rows_affected();
            total_attempted += chunk.len() as u64;
        }

        debug!(
            "repositories merged: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(())
    }

    pub async fn merge_files<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = File> + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s.chunks(self.chunk_size);

        while let Some(chunk) = chunk_stream.next().await {
            if chunk.is_empty() {
                continue;
            }

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT OR IGNORE INTO files (uuid, path, object_id, valid_from) VALUES {}",
                placeholders
            );

            let mut query = sqlx::query(&query_str);

            for file in &chunk {
                query = query
                    .bind(&file.uuid)
                    .bind(&file.path)
                    .bind(&file.object_id)
                    .bind(&file.valid_from)
            }

            let result = query.execute(&self.pool).await?;
            total_inserted += result.rows_affected();
            total_attempted += chunk.len() as u64;
        }

        debug!(
            "files merged: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(())
    }

    pub async fn merge_blobs<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = Blob> + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s.chunks(self.chunk_size);

        while let Some(chunk) = chunk_stream.next().await {
            if chunk.is_empty() {
                continue;
            }

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT OR IGNORE INTO blobs (uuid, repo_id, object_id, has_blob, valid_from) VALUES {}",
                placeholders
            );

            let mut query = sqlx::query(&query_str);

            for file in &chunk {
                query = query
                    .bind(&file.uuid)
                    .bind(&file.repo_id)
                    .bind(&file.object_id)
                    .bind(&file.has_blob)
                    .bind(&file.valid_from)
            }

            let result = query.execute(&self.pool).await?;
            total_inserted += result.rows_affected();
            total_attempted += chunk.len() as u64;
        }

        debug!(
            "blobs merged: attempted={} inserted={}",
            total_attempted, total_inserted
        );
        Ok(())
    }

    pub async fn lookup_repository(&self, repo_id: String) -> Result<Repository, sqlx::Error> {
        sqlx::query_as::<_, Repository>(
            "
                SELECT COALESCE(r.repo_id, ?) as repo_id,
                       COALESCE(r.last_file_index, -1) as last_file_index,
                       COALESCE(r.last_blob_index, -1) as last_blob_index
                FROM (SELECT NULL) n
                         LEFT JOIN repositories r ON r.repo_id = ?
                LIMIT 1
            ",
        )
        .bind(&repo_id)
        .bind(&repo_id)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn update_last_indices(&self) -> Result<Repository, sqlx::Error> {
        sqlx::query_as::<_, Repository>(
            "
            INSERT INTO repositories (repo_id, last_file_index, last_blob_index)
            SELECT
                (SELECT repo_id FROM current_repository LIMIT 1),
                (SELECT COALESCE(MAX(id), -1) FROM files),
                (SELECT COALESCE(MAX(id), -1) FROM blobs)
            ON CONFLICT(repo_id) DO UPDATE
            SET
                last_file_index = MAX(excluded.last_file_index, repositories.last_file_index),
                last_blob_index = MAX(excluded.last_blob_index, repositories.last_blob_index)
            RETURNING repo_id, last_file_index, last_blob_index;
            ",
        )
        .fetch_one(&self.pool)
        .await
    }

    pub fn desired_filesystem_state(
        &self,
        repo_id: String,
    ) -> DBOutputStream<FilePathWithObjectId> {
        self.stream(
            query(
                "
                SELECT
                    path,
                    object_id
                FROM repository_filesystem_available_files
                WHERE repo_id = ?;",
            )
            .bind(repo_id),
        )
    }

    pub fn missing_blobs(
        &self,
        source_repo_id: String,
        target_repo_id: String,
    ) -> DBOutputStream<BlobObjectId> {
        self.stream(
            query(
                "
                SELECT
                    b.object_id
                FROM latest_available_blobs b
                WHERE
                      b.repo_id = ?
                  AND b.object_id NOT IN (
                    SELECT
                        object_id
                    FROM latest_available_blobs
                    WHERE
                        repo_id = ?
                );",
            )
            .bind(source_repo_id)
            .bind(target_repo_id),
        )
    }
}
