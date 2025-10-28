use chrono::prelude::{DateTime, Utc};
use sqlx::Type;
use sqlx::sqlite::SqliteRow;
use sqlx::{FromRow, Row};

#[derive(Debug, FromRow, Clone)]
pub struct CurrentRepository {
    pub repo_id: String,
}

#[derive(Debug, FromRow, Clone)]
pub struct Repository {
    pub repo_id: String,
    pub last_file_index: i64,
    pub last_blob_index: i64,
    pub last_name_index: i64,
}

#[derive(Debug, FromRow, Clone, serde::Serialize, serde::Deserialize)]
pub struct File {
    pub uid: u64,
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

#[derive(Debug, FromRow, Clone, serde::Serialize, serde::Deserialize)]
pub struct Blob {
    pub uid: u64,
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

#[derive(Debug, FromRow, Clone, serde::Serialize, serde::Deserialize)]
pub struct RepositoryName {
    pub uid: u64,
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InsertMaterialisation {
    pub path: String,
    pub blob_id: Option<String>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct InsertFileBundle {
    pub file: InsertFile,
    pub blob: InsertBlob,
    pub materialisation: InsertMaterialisation,
}

#[derive(Debug, FromRow, Clone)]
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

impl<'r> FromRow<'r, SqliteRow> for BlobAssociatedToFiles {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        let blob_id: String = row.try_get("blob_id")?;
        let paths_json: String = row.try_get("paths")?;
        let paths: Vec<String> = serde_json::from_str(&paths_json)
            .map_err(|e| sqlx::Error::Decode(format!("JSON decode error: {e}").into()))?;
        let repo_names_json: String = row.try_get("repositories_with_blob")?;
        let repositories_with_blob: Vec<String> = serde_json::from_str(&repo_names_json)
            .map_err(|e| sqlx::Error::Decode(format!("JSON decode error: {e}").into()))?;
        Ok(BlobAssociatedToFiles {
            blob_id,
            paths,
            repositories_with_blob,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Type, Clone, Hash)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
pub enum VirtualFileState {
    New,
    Ok,
    OkMaterialisationMissing,
    Altered,
    Outdated,
    NeedsCheck,
    CorruptionDetected,
}

#[derive(Debug, Clone)]
pub enum Observation {
    FileSeen(FileSeen),
    FileCheck(FileCheck),
}

#[derive(Debug, FromRow, Clone)]
pub struct FileSeen {
    pub path: String,
    pub seen_id: i64,
    pub seen_dttm: i64,
    pub last_modified_dttm: i64,
    pub size: i64,
}

#[derive(Debug, FromRow, Clone)]
pub struct FileCheck {
    pub path: String,
    pub check_dttm: i64,
    pub hash: String,
}

#[derive(Debug, FromRow, Clone)]
pub struct VirtualFile {
    pub path: String,
    pub materialisation_last_blob_id: Option<String>,
    pub local_has_target_blob: bool,
    pub target_blob_id: Option<String>,
    pub state: VirtualFileState,
}

#[derive(Debug, FromRow, Clone)]
pub struct MissingFile {
    pub path: String,
    pub target_blob_id: String,
    pub local_has_target_blob: bool,
}

#[derive(Debug, PartialEq, Eq, Type, Clone, Hash)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum ConnectionType {
    Local,
    Ssh,
    RClone,
}

#[derive(Debug, FromRow, Clone)]
pub struct Connection {
    pub name: String,
    pub connection_type: ConnectionType,
    pub parameter: String,
}

#[derive(Debug, FromRow, Clone)]
pub struct BlobTransferItem {
    pub transfer_id: u32,
    pub blob_id: String,
    pub path: String,
}

#[derive(Debug, FromRow, Clone)]
pub struct FileTransferItem {
    pub transfer_id: u32,
    pub blob_id: String,
    pub blob_size: i64,
    pub path: String,
}

#[derive(Debug, FromRow, Clone)]
pub struct CopiedTransferItem {
    pub transfer_id: u32,
    pub path: String,
}

#[derive(Debug, FromRow, Clone)]
pub struct WalCheckpoint {
    #[allow(dead_code)]
    pub busy: i64,
    #[allow(dead_code)]
    pub log: i64,
    #[allow(dead_code)]
    pub checkpointed: i64,
}

#[derive(Debug, PartialEq, Eq, Type, Clone, Hash)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum PathType {
    File,
    Dir,
    Unknown,
}

#[derive(Debug, FromRow, Clone)]
pub struct MoveInstr {
    pub src_path: String,
    pub dst_path: String,
    pub blob_id: String,
}

#[derive(Debug, PartialEq, Eq, Type, Clone, Copy, Hash)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
pub enum MoveViolationCode {
    SourceNotFound,
    DestinationExistsDb,
    SourceEqualsDestination,
}

#[derive(Debug, FromRow, Clone)]
pub struct MoveViolation {
    pub code: MoveViolationCode,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub enum MoveEvent {
    Violation(MoveViolation),
    Instruction(MoveInstr),
}

#[derive(Debug, PartialEq, Eq, Type, Clone, Copy, Hash)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
pub enum RmViolationCode {
    SourceNotFound,
}

#[derive(Debug, FromRow, Clone)]
pub struct RmViolation {
    pub code: RmViolationCode,
    pub detail: String,
}

#[derive(Debug, FromRow, Clone)]
pub struct RmInstr {
    pub path: String,
    pub target_blob_id: String,
}

#[derive(Debug, Clone)]
pub enum RmEvent {
    Violation(RmViolation),
    Instruction(RmInstr),
}
