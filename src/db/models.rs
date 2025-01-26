use chrono::prelude::{DateTime, Utc};
use sqlx::sqlite::SqliteRow;
use sqlx::Type;
use sqlx::{FromRow, Row};
use std::fmt::{Display, Formatter};

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

#[derive(Debug)]
pub struct BlobWithPaths {
    pub blob_id: String,
    pub paths: Vec<String>,
}

impl<'r> FromRow<'r, SqliteRow> for BlobWithPaths {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        let blob_id: String = row.try_get("blob_id")?;
        let paths_json: String = row.try_get("paths")?;
        let paths: Vec<String> = serde_json::from_str(&paths_json)
            .map_err(|e| sqlx::Error::Decode(format!("JSON decode error: {}", e).into()))?;
        Ok(BlobWithPaths { blob_id, paths })
    }
}

#[derive(Debug, FromRow)]
pub struct FilePathWithBlobId {
    pub path: String,
    pub blob_id: String,
}

#[derive(Debug, PartialEq, Eq, Type, Clone, Hash)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
pub enum VirtualFileState {
    New,
    Dirty,
    Ok,
    NeedsCheck,
    Deleted,
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

#[derive(Debug, FromRow, Clone)]
pub struct VirtualFile {
    pub path: String,
    #[allow(dead_code)]
    pub file_last_seen_id: Option<i64>,
    #[allow(dead_code)]
    pub file_last_seen_dttm: Option<i64>,
    #[allow(dead_code)]
    pub file_last_modified_dttm: Option<i64>,
    #[allow(dead_code)]
    pub file_size: Option<i64>,
    #[allow(dead_code)]
    pub local_has_blob: bool,
    #[allow(dead_code)]
    pub blob_id: Option<String>,
    #[allow(dead_code)]
    pub blob_size: Option<i64>,
    #[allow(dead_code)]
    pub last_file_eq_blob_check_dttm: Option<i64>,
    #[allow(dead_code)]
    pub last_file_eq_blob_result: Option<bool>,
    pub state: Option<VirtualFileState>,
}

#[derive(Debug)]
pub struct InsertVirtualFile {
    pub path: String,
    pub file_last_seen_id: Option<i64>,
    pub file_last_seen_dttm: Option<i64>,
    pub file_last_modified_dttm: Option<i64>,
    pub file_size: Option<i64>,
    pub last_file_eq_blob_check_dttm: Option<i64>,
    pub last_file_eq_blob_result: Option<bool>,
}

#[derive(Debug, PartialEq, Eq, Type, Clone, Hash)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum ConnectionType {
    Local,
}

impl Display for ConnectionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionType::Local => write!(f, "local"),
        }
    }
}

#[derive(Debug, FromRow)]
pub struct Connection {
    pub name: String,
    pub connection_type: ConnectionType,
    pub parameter: String,
}

#[derive(Debug, FromRow, Clone)]
pub struct TransferItem {
    pub transfer_id: u32,
    pub blob_id: String,
    pub path: String,
}
