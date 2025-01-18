use chrono::prelude::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub struct CurrentRepository {
    pub repo_id: String,
}

#[derive(Debug, FromRow)]
pub struct Repository {
    pub repo_id: String,
    pub last_file_index: i32,
    pub last_blob_index: i32,
}

#[derive(Debug, FromRow)]
pub struct File {
    pub uuid: String,
    pub path: String,
    pub object_id: Option<String>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug)]
pub struct InputFile {
    pub path: String,
    pub object_id: Option<String>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
pub struct Blob {
    pub uuid: String,
    pub repo_id: String,
    pub object_id: String,
    pub has_blob: bool,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug)]
pub struct InputBlob {
    pub repo_id: String,
    pub object_id: String,
    pub has_blob: bool,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
pub struct BlobObjectId {
    pub object_id: String,
}

#[derive(Debug, FromRow)]
pub struct FilePathWithObjectId {
    pub path: String,
    pub object_id: String,
}
