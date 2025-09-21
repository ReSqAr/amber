use chrono::prelude::{DateTime, Utc};
use duckdb::ToSql;
use duckdb::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, Value, ValueRef};

#[derive(Debug, Clone)]
pub struct CurrentRepository {
    pub repo_id: String,
}

#[derive(Debug, Clone)]
pub struct Repository {
    pub repo_id: String,
    pub last_file_index: i32,
    pub last_blob_index: i32,
    pub last_name_index: i32,
}

#[derive(Debug, Clone)]
pub struct File {
    pub uuid: String,
    pub path: String,
    pub blob_id: Option<String>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct InsertFile {
    pub path: String,
    pub blob_id: Option<String>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct Blob {
    pub uuid: String,
    pub repo_id: String,
    pub blob_id: String,
    pub blob_size: i64,
    pub has_blob: bool,
    pub path: Option<String>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct InsertBlob {
    pub repo_id: String,
    pub blob_id: String,
    pub blob_size: i64,
    pub has_blob: bool,
    pub path: Option<String>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ObservedBlob {
    pub repo_id: String,
    pub has_blob: bool,
    pub path: String,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct RepositoryName {
    pub uuid: String,
    pub repo_id: String,
    pub name: String,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct InsertRepositoryName {
    pub repo_id: String,
    pub name: String,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct InsertMaterialisation {
    pub path: String,
    pub blob_id: Option<String>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct Materialisation {
    #[allow(dead_code)]
    pub path: String,
    pub blob_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AvailableBlob {
    pub repo_id: String,
    pub blob_id: String,
    pub blob_size: i64,
    pub path: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BlobAssociatedToFiles {
    pub blob_id: String,
    pub paths: Vec<String>,
    pub repositories_with_blob: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum VirtualFileState {
    New,
    Ok,
    OkMaterialisationMissing,
    Altered,
    Outdated,
    NeedsCheck,
}

impl ToSql for VirtualFileState {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, duckdb::Error> {
        let s = match self {
            VirtualFileState::New => "new",
            VirtualFileState::Ok => "ok",
            VirtualFileState::OkMaterialisationMissing => "ok_materialisation_missing",
            VirtualFileState::Altered => "altered",
            VirtualFileState::Outdated => "outdated",
            VirtualFileState::NeedsCheck => "needs_check",
        };
        Ok(ToSqlOutput::Owned(Value::Text(s.to_string())))
    }
}

impl FromSql for VirtualFileState {
    fn column_result(v: ValueRef<'_>) -> FromSqlResult<Self> {
        match v.as_str()? {
            "new" => Ok(VirtualFileState::New),
            "ok" => Ok(VirtualFileState::Ok),
            "ok_materialisation_missing" => Ok(VirtualFileState::OkMaterialisationMissing),
            "altered" => Ok(VirtualFileState::Altered),
            "outdated" => Ok(VirtualFileState::Outdated),
            "needs_check" => Ok(VirtualFileState::NeedsCheck),
            other => Err(FromSqlError::Other(
                format!("unknown VirtualFileState: {other}").into(),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Observation {
    FileSeen(FileSeen),
    FileCheck(FileCheck),
}

#[derive(Debug, Clone)]
pub struct FileSeen {
    pub path: String,
    pub seen_id: i64,
    pub seen_dttm: i64,
    pub last_modified_dttm: i64,
    pub size: i64,
}

#[derive(Debug, Clone)]
pub struct FileCheck {
    pub path: String,
    pub check_dttm: i64,
    pub hash: String,
}

#[derive(Debug, Clone)]
pub struct VirtualFile {
    pub path: String,
    pub materialisation_last_blob_id: Option<String>,
    pub local_has_target_blob: bool,
    pub target_blob_id: Option<String>,
    pub state: VirtualFileState,
}
#[derive(Debug, Clone)]
pub struct MissingFile {
    pub path: String,
    pub target_blob_id: String,
    pub local_has_target_blob: bool,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum ConnectionType {
    Local,
    Ssh,
    RClone,
}

impl ToSql for ConnectionType {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, duckdb::Error> {
        let s = match self {
            ConnectionType::Local => "local",
            ConnectionType::Ssh => "ssh",
            ConnectionType::RClone => "rclone",
        };
        Ok(ToSqlOutput::Owned(Value::Text(s.to_string())))
    }
}

impl FromSql for ConnectionType {
    fn column_result(v: ValueRef<'_>) -> FromSqlResult<Self> {
        match v.as_str()? {
            "local" => Ok(ConnectionType::Local),
            "ssh" => Ok(ConnectionType::Ssh),
            "rclone" => Ok(ConnectionType::RClone),
            other => Err(FromSqlError::Other(
                format!("unknown ConnectionType: {other}").into(),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Connection {
    pub name: String,
    pub connection_type: ConnectionType,
    pub parameter: String,
}

#[derive(Debug, Clone)]
pub struct BlobTransferItem {
    pub transfer_id: u32,
    pub blob_id: String,
    pub path: String,
}

#[derive(Debug, Clone)]
pub struct FileTransferItem {
    pub transfer_id: u32,
    pub blob_id: String,
    pub blob_size: i64,
    pub path: String,
}

#[derive(Debug, Clone)]
pub struct CopiedTransferItem {
    pub transfer_id: u32,
    pub path: String,
}
