use crate::db::models::{
    Blob, BlobObjectId, CurrentRepository, File, FilePathWithObjectId, Repository,
};
use chrono::{DateTime, Utc};
use log::debug;
use sqlx::SqlitePool;
use uuid::Uuid;

pub struct DB {
    pool: SqlitePool,
}

impl DB {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
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

    pub async fn add_file(
        &self,
        path: &str,
        object_id: Option<&str>,
        valid_from: DateTime<Utc>,
    ) -> Result<File, sqlx::Error> {
        let uuid = Uuid::new_v4().to_string();
        sqlx::query_as::<_, File>(
            "INSERT OR IGNORE INTO files (uuid, path, object_id, valid_from) VALUES (?, ?, ?, ?)
            RETURNING uuid, path, object_id, valid_from",
        )
        .bind(uuid)
        .bind(path)
        .bind(object_id)
        .bind(valid_from)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn add_blob(
        &self,
        repo_id: &str,
        object_id: &str,
        valid_from: DateTime<Utc>,
        file_exists: bool,
    ) -> Result<Blob, sqlx::Error> {
        let uuid = Uuid::new_v4().to_string();
        sqlx::query_as::<_, Blob>(
            "INSERT OR IGNORE INTO blobs (uuid, repo_id, object_id, file_exists, valid_from) VALUES (?, ?, ?, ?, ?)
            RETURNING uuid, repo_id, object_id, file_exists, valid_from",
        )
        .bind(uuid)
        .bind(repo_id)
        .bind(object_id)
        .bind(file_exists)
        .bind(valid_from)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn select_repositories(&self) -> Result<Vec<Repository>, sqlx::Error> {
        sqlx::query_as::<_, Repository>(
            "
                SELECT repo_id, last_file_index, last_blob_index
                FROM repositories",
        )
        .fetch_all(&self.pool)
        .await
    }

    pub async fn select_files(&self, last_index: &i32) -> Result<Vec<File>, sqlx::Error> {
        sqlx::query_as::<_, File>(
            "
                SELECT uuid, path, object_id, valid_from
                FROM files
                WHERE id > ?",
        )
        .bind(last_index)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn select_blobs(&self, last_index: &i32) -> Result<Vec<Blob>, sqlx::Error> {
        sqlx::query_as::<_, Blob>(
            "
                SELECT uuid, repo_id, object_id, file_exists, valid_from
                FROM blobs
                WHERE id > ?",
        )
        .bind(last_index)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn merge_repositories(
        &self,
        repositories: Vec<Repository>,
    ) -> Result<(), sqlx::Error> {
        debug!("merging {} repositories", repositories.len());
        for repo in repositories {
            let _ = sqlx::query(
                "INSERT INTO repositories (repo_id, last_file_index, last_blob_index)
                VALUES (?, ?, ?)
                ON CONFLICT(repo_id) DO UPDATE SET
                    last_file_index = max(last_file_index, excluded.last_file_index),
                    last_blob_index = max(last_blob_index, excluded.last_blob_index)
                ",
            )
            .bind(repo.repo_id)
            .bind(repo.last_file_index)
            .bind(repo.last_blob_index)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    pub async fn merge_files(&self, files: Vec<File>) -> Result<(), sqlx::Error> {
        debug!("merging {} files", files.len());
        for file in files {
            let _ = sqlx::query("INSERT OR IGNORE INTO files (uuid, path, object_id, valid_from) VALUES (?, ?, ?, ?)")
                .bind(file.uuid)
                .bind(file.path)
                .bind(file.object_id)
                .bind(file.valid_from)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    pub async fn merge_blobs(&self, blobs: Vec<Blob>) -> Result<(), sqlx::Error> {
        debug!("merging {} blobs", blobs.len());
        for blob in blobs {
            let _ = sqlx::query("INSERT OR IGNORE INTO blobs (uuid, repo_id, object_id, file_exists, valid_from) VALUES (?, ?, ?, ?, ?)")
                .bind(blob.uuid)
                .bind(blob.repo_id)
                .bind(blob.object_id)
                .bind(blob.file_exists)
                .bind(blob.valid_from)
                .execute(&self.pool)
                .await?;
        }
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

    pub async fn desired_filesystem_state(
        &self,
        repo_id: &str,
    ) -> Result<Vec<FilePathWithObjectId>, sqlx::Error> {
        sqlx::query_as::<_, FilePathWithObjectId>(
            "
            SELECT
                path,
                object_id
            FROM repository_filesystem_available_files
            WHERE repo_id = ?;
            ",
        )
        .bind(repo_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn missing_blobs(
        &self,
        source_repo_id: &str,
        target_repo_id: &str,
    ) -> Result<Vec<BlobObjectId>, sqlx::Error> {
        sqlx::query_as::<_, BlobObjectId>(
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
                );
            ",
        )
        .bind(source_repo_id)
        .bind(target_repo_id)
        .fetch_all(&self.pool)
        .await
    }
}
