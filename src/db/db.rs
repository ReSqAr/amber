use crate::db::models::{Blob, CurrentRepository, File, Repository};
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use uuid::Uuid;

pub struct DB {
    pool: SqlitePool,
}

impl DB {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn get_or_create_current_repository(&self) -> Result<CurrentRepository, sqlx::Error> {
        let potential_new_repository_id = Uuid::new_v4().to_string();
        if let Some(repo) = sqlx::query_as::<_, CurrentRepository>(
            "
INSERT OR IGNORE INTO current_repository (id, repo_id)
VALUES (1, ?)
RETURNING id, repo_id;
                    ",
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
            "INSERT OR IGNORE INTO files (uuid, path, object_id, valid_from) VALUES (?, ?, ?, ?) RETURNING *",
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
            "INSERT OR IGNORE INTO blobs (uuid, repo_id, object_id, valid_from, file_exists) VALUES (?, ?, ?, ?, ?) RETURNING *",
        )
        .bind(uuid)
        .bind(repo_id)
        .bind(object_id)
        .bind(valid_from)
        .bind(file_exists)
        .fetch_one(&self.pool)
        .await
    }
}
