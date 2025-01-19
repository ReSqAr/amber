use chrono::prelude::{DateTime, Utc};
use sqlx::FromRow;
use sqlx::Type;

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
    pub blob_id: Option<String>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug)]
pub struct InsertFile {
    pub path: String,
    pub blob_id: Option<String>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
pub struct Blob {
    pub uuid: String,
    pub repo_id: String,
    pub blob_id: String,
    pub blob_size: i64,
    pub has_blob: bool,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug)]
pub struct InsertBlob {
    pub repo_id: String,
    pub blob_id: String,
    pub blob_size: i64,
    pub has_blob: bool,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
pub struct BlobId {
    pub blob_id: String,
}

#[derive(Debug, FromRow)]
pub struct FilePathWithBlobId {
    pub path: String,
    pub blob_id: String,
}

#[derive(Debug, PartialEq, Eq, Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
pub enum FileState {
    New,
    Dirty,
    Ok,
    NeedsCheck,
}


#[derive(Debug)]
pub enum Observation {
    FileSeen(FileSeen),
    FileEqBlobCheck(FileEqBlobCheck),
}

#[derive(Debug, FromRow)]
pub struct FileSeen {
    pub path: String,
    pub last_seen_id: i64,
    pub last_seen_dttm: i64,
    pub last_modified_dttm: i64,
    pub size: i64,
}

#[derive(Debug, FromRow)]
pub struct FileEqBlobCheck {
    pub path: String,
    pub last_check_dttm: i64,
    pub last_result: bool,
}

#[derive(Debug, FromRow)]
pub struct VirtualFile {
    pub path: String,
    pub file_last_seen_id: Option<i64>,
    pub file_last_seen_dttm: Option<i64>,
    pub file_last_modified_dttm: Option<i64>,
    pub file_size: Option<i64>,
    pub local_has_blob: bool,
    pub blob_id: Option<String>,
    pub blob_size: Option<i64>,
    pub last_file_eq_blob_check_dttm: Option<i64>,
    pub last_file_eq_blob_result: bool,
    pub state: Option<FileState>,
}

#[derive(Debug)]
pub struct InsertVirtualFile {
    pub path: String,
    pub file_last_seen_id: Option<i64>,
    pub file_last_seen_dttm: Option<i64>,
    pub file_last_modified_dttm: Option<i64>,
    pub file_size: Option<i64>,
    pub last_file_eq_blob_check_dttm: Option<i64>,
    pub last_file_eq_blob_result: Option<bool>, // Option = option not to update
}
