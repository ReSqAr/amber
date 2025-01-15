use sqlx::FromRow;
use chrono::prelude::{DateTime, Utc};

#[derive(Debug, FromRow)]
pub struct CurrentRepository {
    pub id: i32,
    pub repo_id: String,
}

#[derive(Debug, FromRow)]
pub struct Repository {
    pub id: i32,
    pub repo_id: String,
    pub last_file_index: i32,
    pub last_blob_index: i32,
}

#[derive(Debug, FromRow)]
pub struct File {
    pub id: i32,
    pub uuid: String,
    pub path: String,
    pub object_id: Option<String>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
pub struct Blob {
    pub id: i32,
    pub uuid: String,
    pub repo_id: String,
    pub object_id: String,
    pub valid_from: DateTime<Utc>,
    pub file_exists: bool,
}
