use crate::db::models::{
    Blob, BlobId, BlobWithPaths, CurrentRepository, File, FileEqBlobCheck, FilePathWithBlobId,
    FileSeen, InsertBlob, InsertFile, InsertVirtualFile, Observation, Repository, VirtualFile,
};
use crate::utils::flow::{ExtFlow, Flow};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use log::debug;
use sqlx::query::Query;
use sqlx::sqlite::SqliteArguments;
use sqlx::{query, Either, Executor, FromRow, Sqlite, SqlitePool};
use uuid::Uuid;

#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
    chunk_size: usize,
}

pub(crate) type DBOutputStream<'a, T> = BoxStream<'a, Result<T, sqlx::Error>>;

impl Database {
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

    pub async fn add_files<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = InsertFile> + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s.ready_chunks(self.chunk_size);

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

    pub async fn add_blobs<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = InsertBlob> + Unpin,
    {
        let mut total_attempted: u64 = 0;
        let mut total_inserted: u64 = 0;
        let mut chunk_stream = s.ready_chunks(self.chunk_size);

        while let Some(chunk) = chunk_stream.next().await {
            if chunk.is_empty() {
                continue;
            }

            let placeholders = chunk
                .iter()
                .map(|_| "(?, ?, ?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT INTO blobs (uuid, repo_id, blob_id, blob_size, has_blob, valid_from) VALUES {}",
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
                    .bind(blob.valid_from);
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

    pub async fn select_repositories(&self) -> DBOutputStream<'static, Repository> {
        self.stream(query(
            "SELECT repo_id, last_file_index, last_blob_index FROM repositories",
        ))
    }

    pub async fn select_files(&self, last_index: i32) -> DBOutputStream<'static, File> {
        self.stream(
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
            query(
                "
                SELECT uuid, repo_id, blob_id, blob_size, has_blob, valid_from
                FROM blobs
                WHERE id > ?",
            )
            .bind(last_index),
        )
    }

    pub async fn merge_repositories<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = Repository> + Unpin + Send,
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
                    .bind(repo.last_file_index)
                    .bind(repo.last_blob_index)
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
        S: Stream<Item = File> + Unpin + Send,
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
        S: Stream<Item = Blob> + Unpin + Send,
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
                .map(|_| "(?, ?, ?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");

            let query_str = format!(
                "INSERT OR IGNORE INTO blobs (uuid, repo_id, blob_id, blob_size, has_blob, valid_from) VALUES {}",
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
                    .bind(file.valid_from)
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

    pub fn target_filesystem_state(&self, repo_id: String) -> DBOutputStream<FilePathWithBlobId> {
        self.stream(
            query(
                "
                SELECT
                    path,
                    blob_id
                FROM repository_filesystem_available_files
                WHERE repo_id = ?;",
            )
            .bind(repo_id),
        )
    }

    pub(crate) fn missing_blobs(
        &self,
        repo_id: String,
    ) -> impl Stream<Item = Result<BlobWithPaths, sqlx::Error>> + Unpin + Send + Sized {
        self.stream(
            query(
                "
                WITH filtered_blobs AS (
                    SELECT blob_id
                    FROM latest_available_blobs
                    WHERE repo_id = ?
                )
                SELECT
                    f.blob_id,
                    json_group_array(f.path) AS paths
                FROM latest_filesystem_files f
                         LEFT JOIN filtered_blobs b
                                   ON f.blob_id = b.blob_id
                WHERE b.blob_id IS NULL
                GROUP BY f.blob_id;
            ",
            )
            .bind(repo_id),
        )
    }

    pub fn deprecated_missing_blobs(
        &self,
        source_repo_id: String,
        target_repo_id: String,
    ) -> DBOutputStream<BlobId> {
        self.stream(
            query(
                "
                SELECT
                    b.blob_id
                FROM latest_available_blobs b
                WHERE
                      b.repo_id = ?
                  AND b.blob_id NOT IN (
                    SELECT
                        blob_id
                    FROM latest_available_blobs
                    WHERE
                        repo_id = ?
                );",
            )
            .bind(source_repo_id)
            .bind(target_repo_id),
        )
    }

    pub async fn refresh_virtual_filesystem(&self) -> Result<(), sqlx::Error> {
        let query = "
            INSERT OR REPLACE INTO virtual_filesystem (
                path,
                file_last_seen_id,
                file_last_seen_dttm,
                file_last_modified_dttm,
                file_size,
                local_has_blob,
                blob_id,
                blob_size,
                last_file_eq_blob_check_dttm,
                last_file_eq_blob_result,
                state
            )
            SELECT
                f.path,
                vfs.file_last_seen_id,
                vfs.file_last_seen_dttm,
                vfs.file_last_modified_dttm,
                vfs.file_size,
                CASE
                    WHEN a.blob_id IS NOT NULL THEN TRUE
                    ELSE FALSE
                    END AS local_has_blob,
                a.blob_id,
                a.blob_size,
                vfs.last_file_eq_blob_check_dttm,
                CASE
                    WHEN vfs.blob_id = f.blob_id THEN vfs.last_file_eq_blob_result
                    ELSE FALSE
                    END,
                vfs.state
            FROM latest_filesystem_files f
                 LEFT JOIN (SELECT blob_id, blob_size FROM latest_available_blobs INNER JOIN current_repository USING (repo_id)) a ON f.blob_id = a.blob_id
                 LEFT JOIN virtual_filesystem vfs ON vfs.path = f.path;
     ";

        let result = sqlx::query(query).execute(&self.pool).await?;
        debug!(
            "refresh_virtual_filesystem: rows affected={}",
            result.rows_affected()
        );

        Ok(())
    }
    pub async fn cleanup_virtual_filesystem(&self, last_seen_id: i64) -> Result<(), sqlx::Error> {
        let query = "
        DELETE FROM virtual_filesystem
        WHERE file_last_seen_id IS NOT NULL AND file_last_seen_id != ? AND blob_id IS NULL;
    ";

        let result = sqlx::query(query)
            .bind(last_seen_id)
            .execute(&self.pool)
            .await?;
        debug!(
            "cleanup_virtual_filesystem: rows affected={}",
            result.rows_affected()
        );

        Ok(())
    }

    pub async fn select_deleted_files_on_virtual_filesystem(
        &self,
        last_seen_id: i64,
    ) -> DBOutputStream<'static, VirtualFile> {
        self.stream(
            query(
                "
            UPDATE virtual_filesystem
            SET state = 'deleted'
            WHERE (file_last_seen_id != ? OR file_last_seen_id IS NULL) AND local_has_blob
            RETURNING
                path,
                file_last_seen_id,
                file_last_seen_dttm,
                file_last_modified_dttm,
                file_size,
                local_has_blob,
                blob_id,
                blob_size,
                last_file_eq_blob_check_dttm,
                last_file_eq_blob_result,
                state;",
            )
            .bind(last_seen_id),
        )
    }

    pub async fn add_virtual_filesystem_observations(
        &self,
        input_stream: impl Stream<Item = Flow<Observation>> + Unpin + Send + 'static,
    ) -> impl Stream<Item = ExtFlow<Result<Vec<VirtualFile>, sqlx::Error>>> + Unpin + Send + 'static
    {
        let pool = self.pool.clone();
        input_stream.ready_chunks(self.chunk_size).then(move |chunk: Vec<Flow<Observation>>| {
            let pool = pool.clone();
            Box::pin(async move {
                let shutting_down = chunk.iter().any(
                    |message| matches!(message, Flow::Shutdown)
                );
                let observations: Vec<_> = chunk.iter().filter_map(
                    |message| match message {
                        Flow::Data(observation) => Some(observation),
                        Flow::Shutdown => None,
                    }
                ).collect();

                if observations.is_empty() { // SQL is otherwise not valid
                    return match shutting_down {
                        true => ExtFlow::Shutdown(Ok(vec![])),
                        false => ExtFlow::Data(Ok(vec![]))
                    }
                }

                let placeholders = observations
                    .iter()
                    .map(|_| "(?, ?, ?, ?, ?, ?, ?, 'new')")
                    .collect::<Vec<_>>()
                    .join(", ");

                let query_str = format!("
                    INSERT INTO virtual_filesystem (
                        path,
                        file_last_seen_id,
                        file_last_seen_dttm,
                        file_last_modified_dttm,
                        file_size,
                        last_file_eq_blob_check_dttm,
                        last_file_eq_blob_result,
                        state
                    )
                    VALUES {}
                    ON CONFLICT(path) DO UPDATE SET
                        file_last_seen_id = COALESCE(excluded.file_last_seen_id, file_last_seen_id),
                        file_last_seen_dttm = COALESCE(excluded.file_last_seen_dttm, file_last_seen_dttm),
                        file_last_modified_dttm = COALESCE(excluded.file_last_modified_dttm, file_last_modified_dttm),
                        file_size = COALESCE(excluded.file_size, file_size),
                        last_file_eq_blob_check_dttm = COALESCE(excluded.last_file_eq_blob_check_dttm, last_file_eq_blob_check_dttm),
                        last_file_eq_blob_result = COALESCE(excluded.last_file_eq_blob_result, last_file_eq_blob_result),

                        state = CASE
                                    WHEN blob_id IS NULL THEN 'new'
                                    WHEN COALESCE(excluded.last_file_eq_blob_result, last_file_eq_blob_result, FALSE) = FALSE
                                        AND COALESCE(excluded.file_last_modified_dttm, file_last_modified_dttm)
                                        <= COALESCE(excluded.last_file_eq_blob_check_dttm, last_file_eq_blob_check_dttm) THEN 'dirty'
                                    WHEN (blob_id IS NOT NULL
                                        AND COALESCE(excluded.last_file_eq_blob_result, last_file_eq_blob_result, FALSE) = TRUE
                                        AND COALESCE(excluded.file_last_modified_dttm, file_last_modified_dttm)
                                        <= COALESCE(excluded.last_file_eq_blob_check_dttm, last_file_eq_blob_check_dttm)) THEN 'ok'
                                    ELSE 'needs_check'
                            END
                    RETURNING
                        path,
                        file_last_seen_id,
                        file_last_seen_dttm,
                        file_last_modified_dttm,
                        file_size,
                        local_has_blob,
                        blob_id,
                        blob_size,
                        last_file_eq_blob_check_dttm,
                        last_file_eq_blob_result,
                        state
                    ;
                ", placeholders);

                let mut query = sqlx::query_as::<_, VirtualFile>(&query_str);

                for observation in observations {
                    let ivf: InsertVirtualFile = match observation {
                        Observation::FileSeen(FileSeen {
                                                  path,
                                                  last_seen_id,
                                                  last_seen_dttm,
                                                  last_modified_dttm,
                                                  size,
                                              }) => InsertVirtualFile {
                            path: path.clone(),
                            file_last_seen_id: (*last_seen_id).into(),
                            file_last_seen_dttm: (*last_seen_dttm).into(),
                            file_last_modified_dttm: (*last_modified_dttm).into(),
                            file_size: (*size).into(),
                            last_file_eq_blob_check_dttm: None,
                            last_file_eq_blob_result: None,
                        },
                        Observation::FileEqBlobCheck(FileEqBlobCheck {
                                                         path,
                                                         last_check_dttm,
                                                         last_result,
                                                     }) => InsertVirtualFile {
                            path: path.clone(),
                            file_last_seen_id: None,
                            file_last_seen_dttm: None,
                            file_last_modified_dttm: None,
                            file_size: None,
                            last_file_eq_blob_check_dttm: (*last_check_dttm).into(),
                            last_file_eq_blob_result: (*last_result).into(),
                        },
                    };

                    query = query
                        .bind(ivf.path.clone())
                        .bind(ivf.file_last_seen_id)
                        .bind(ivf.file_last_seen_dttm)
                        .bind(ivf.file_last_modified_dttm)
                        .bind(ivf.file_size)
                        .bind(ivf.last_file_eq_blob_check_dttm)
                        .bind(ivf.last_file_eq_blob_result);
                }


                let result = query.fetch_all(&pool).await;
                match shutting_down {
                    true => ExtFlow::Shutdown(result),
                    false => ExtFlow::Data(result)
                }
            })
        })
    }
}
