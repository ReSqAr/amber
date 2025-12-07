use crate::db::logstore;
use chrono::prelude::{DateTime, Utc};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, Eq, PartialEq, Hash)]
pub struct Uid(pub(crate) u64);
impl From<u64> for Uid {
    fn from(v: u64) -> Self {
        Self(v)
    }
}
impl From<Uid> for u64 {
    fn from(v: Uid) -> Self {
        v.0
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Eq, PartialEq, Hash)]
pub struct Path(pub(crate) String);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Eq, PartialEq, Hash)]
pub struct BlobID(pub(crate) String);

impl BlobID {
    pub fn path(&self) -> PathBuf {
        let blob_id = self.0.clone();
        if blob_id.len() > 6 {
            PathBuf::from(&blob_id[0..2])
                .join(&blob_id[2..4])
                .join(&blob_id[4..])
        } else {
            PathBuf::from(blob_id)
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Eq, PartialEq, Hash)]
pub(crate) struct RepoID(pub(crate) String);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct BlobRef {
    pub(crate) blob_id: BlobID,
    pub(crate) repo_id: RepoID,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct CurrentBlob {
    pub(crate) blob_size: u64,
    pub(crate) blob_path: Option<Path>,
    pub(crate) valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct SizedBlobID {
    pub blob_id: BlobID,
    pub blob_size: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct CurrentFile {
    pub(crate) blob_id: BlobID,
    pub(crate) valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct CurrentMaterialisation {
    pub(crate) blob_id: BlobID,
    pub(crate) valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct CurrentRepositoryName {
    pub(crate) name: String,
    pub(crate) valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct CurrentObservation {
    pub(crate) fs_last_seen_id: i64,
    pub(crate) fs_last_seen_dttm: DateTime<Utc>,
    pub(crate) fs_last_modified_dttm: DateTime<Utc>,
    pub(crate) fs_last_size: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct CurrentCheck {
    pub(crate) check_last_dttm: DateTime<Utc>,
    pub(crate) check_last_hash: BlobID,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CurrentRepository {
    pub repo_id: RepoID,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RepositoryMetadata {
    pub last_file_index: Option<u64>,
    pub last_blob_index: Option<u64>,
    pub last_name_index: Option<u64>,
}

#[derive(
    Debug, Clone, serde::Serialize, serde::Deserialize, Eq, PartialEq, Hash, Ord, PartialOrd,
)]
pub struct ConnectionName(pub(crate) String);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConnectionMetadata {
    pub connection_type: ConnectionType,
    pub parameter: String,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
pub enum ConnectionType {
    Local,
    Ssh,
    RClone,
}
#[derive(Debug, Clone)]
pub struct Connection {
    pub name: ConnectionName,
    pub connection_type: ConnectionType,
    pub parameter: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Eq, PartialEq, Hash)]
pub struct TableName(pub(crate) String);

impl From<&'static str> for TableName {
    fn from(value: &'static str) -> Self {
        Self(value.to_string())
    }
}

#[derive(
    Debug, Clone, Copy, serde::Serialize, serde::Deserialize, Eq, PartialEq, Ord, PartialOrd,
)]
pub struct LogOffset(pub(crate) u64);

impl From<LogOffset> for logstore::Offset {
    fn from(o: LogOffset) -> Self {
        o.0.into()
    }
}

impl From<logstore::Offset> for LogOffset {
    fn from(o: logstore::Offset) -> Self {
        Self(o.into())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlobTransferItem {
    pub transfer_id: u32,
    pub blob_id: BlobID,
    pub blob_size: u64,
    pub path: Path,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileTransferItem {
    pub transfer_id: u32,
    pub blob_id: BlobID,
    pub blob_size: u64,
    pub path: Path,
}

#[derive(Debug, Clone)]
pub struct CopiedTransferItem {
    pub transfer_id: u32,
    pub path: Path,
    pub blob_id: BlobID,
    pub blob_size: u64,
}

#[derive(Debug, Clone)]
pub enum VirtualFileState {
    New,
    Ok {
        file: CurrentFile,
        #[allow(dead_code)]
        blob: CurrentBlob,
    },
    OkMaterialisationMissing {
        file: CurrentFile,
        #[allow(dead_code)]
        blob: CurrentBlob,
    },
    OkBlobMissing {
        file: CurrentFile,
    },
    Altered {
        file: CurrentFile,
        blob: Option<CurrentBlob>,
    },
    Outdated {
        file: Option<CurrentFile>,
        blob: Option<CurrentBlob>,
        #[allow(dead_code)]
        mat: CurrentMaterialisation,
    },
    NeedsCheck,
    CorruptionDetected {
        file: CurrentFile,
        #[allow(dead_code)]
        blob: Option<CurrentBlob>,
    },
}

#[derive(Debug, Clone)]
pub struct VirtualFile {
    pub file_seen: FileSeen,
    pub current_file: Option<CurrentFile>,
    pub current_blob: Option<CurrentBlob>,
    pub current_materialisation: Option<CurrentMaterialisation>,
    pub current_check: Option<CurrentCheck>,
}

impl VirtualFile {
    pub(crate) fn state(&self) -> VirtualFileState {
        if self.current_blob.is_none() && self.current_materialisation.is_none() {
            VirtualFileState::New
        } else if let Some(check) = &self.current_check
            && self.file_seen.last_modified_dttm <= check.check_last_dttm
        {
            // we can trust the check
            if let Some(file) = &self.current_file {
                if check.check_last_hash != file.blob_id {
                    // check says: they are not the same
                    if let Some(mat) = &self.current_materialisation
                        && check.check_last_hash == mat.blob_id
                    {
                        // previously materialised version
                        VirtualFileState::Outdated {
                            file: Some(file.clone()),
                            blob: self.current_blob.clone(),
                            mat: mat.clone(),
                        }
                    } else {
                        VirtualFileState::Altered {
                            file: file.clone(),
                            blob: self.current_blob.clone(),
                        }
                    }
                } else if let Some(blob) = &self.current_blob {
                    if self.file_seen.size != blob.blob_size {
                        // shouldn't have trusted the check that the blob ids are the same
                        VirtualFileState::CorruptionDetected {
                            file: file.clone(),
                            blob: Some(blob.clone()),
                        }
                    } else if let Some(mat) = &self.current_materialisation
                        && mat.blob_id != check.check_last_hash
                    {
                        // Ok: but materialisation needs to be recorded
                        VirtualFileState::OkMaterialisationMissing {
                            file: file.clone(),
                            blob: blob.clone(),
                        }
                    } else {
                        // Ok: hashes are the same and the sizes are the same
                        VirtualFileState::Ok {
                            file: file.clone(),
                            blob: blob.clone(),
                        }
                    }
                } else {
                    // odd - we have the correct file & the materialisation, but the blob is missing?
                    VirtualFileState::OkBlobMissing { file: file.clone() }
                }
            } else if let Some(mat) = &self.current_materialisation {
                // previously materialised version of deleted file
                VirtualFileState::Outdated {
                    file: None,
                    blob: self.current_blob.clone(),
                    mat: mat.clone(),
                }
            } else {
                VirtualFileState::New
            }
        } else {
            // check not trustworthy, check again
            VirtualFileState::NeedsCheck
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct File {
    pub uid: Uid,
    pub path: Path,
    pub blob_id: Option<BlobID>,
    pub valid_from: DateTime<Utc>,
}

impl File {
    pub fn from_insert_file(file: InsertFile, uid: Uid) -> Self {
        Self {
            uid,
            path: file.path,
            blob_id: file.blob_id,
            valid_from: file.valid_from,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InsertFile {
    pub path: Path,
    pub blob_id: Option<BlobID>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Blob {
    pub uid: Uid,
    pub repo_id: RepoID,
    pub blob_id: BlobID,
    pub blob_size: u64,
    pub has_blob: bool,
    pub path: Option<Path>,
    pub valid_from: DateTime<Utc>,
}

impl Blob {
    pub fn from_insert_blob(blob: InsertBlob, uid: Uid) -> Self {
        Self {
            uid,
            repo_id: blob.repo_id,
            blob_id: blob.blob_id,
            blob_size: blob.blob_size,
            has_blob: blob.has_blob,
            path: blob.path,
            valid_from: blob.valid_from,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InsertBlob {
    pub repo_id: RepoID,
    pub blob_id: BlobID,
    pub blob_size: u64,
    pub has_blob: bool,
    pub path: Option<Path>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct Repository {
    pub repo_id: RepoID,
    pub last_file_index: Option<u64>,
    pub last_blob_index: Option<u64>,
    pub last_name_index: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RepositoryName {
    pub uid: Uid,
    pub repo_id: RepoID,
    pub name: String,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct InsertRepositoryName {
    pub repo_id: RepoID,
    pub name: String,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RawRepositoryName {
    pub name: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InsertMaterialisation {
    pub path: Path,
    pub blob_id: Option<BlobID>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct InsertFileBundle {
    pub file: InsertFile,
    pub blob: InsertBlob,
    pub materialisation: InsertMaterialisation,
}

#[derive(Debug, Clone)]
pub struct AvailableBlob {
    pub repo_id: RepoID,
    pub blob_id: BlobID,
    pub blob_size: u64,
    pub path: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BlobAssociatedToFiles {
    pub blob_id: BlobID,
    pub path: Path,
    pub repositories_with_blob: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FileSeen {
    pub path: Path,
    pub seen_id: i64,
    pub seen_dttm: DateTime<Utc>,
    pub last_modified_dttm: DateTime<Utc>,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct FileCheck {
    pub path: Path,
    pub check_dttm: DateTime<Utc>,
    pub hash: BlobID,
}

#[derive(Debug, Clone)]
pub struct MissingFile {
    pub path: Path,
    pub target_blob_id: BlobID,
    pub local_has_target_blob: bool,
}
