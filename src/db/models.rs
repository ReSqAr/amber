use crate::db::stores::log;
use crate::db::stores::reduced::Always;
use crate::db::stores::reduced::{RowStatus, Status, ValidFrom};
use chrono::prelude::{DateTime, Utc};
use std::path::PathBuf;

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Uid(pub u64);
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

#[derive(
    Debug, Clone, serde::Serialize, serde::Deserialize, Eq, PartialEq, Hash, Ord, PartialOrd,
)]
pub struct Path(pub String);

impl AsRef<str> for Path {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Eq, PartialEq, Hash)]
pub struct BlobID(pub String);

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
pub struct RepoID(pub String);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BlobRef {
    pub blob_id: BlobID,
    pub repo_id: RepoID,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SizedBlobID {
    pub blob_id: BlobID,
    pub blob_size: u64,
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BlobMeta {
    pub size: u64,
    pub path: Option<Path>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HasBlob(pub bool);

impl Status for HasBlob {
    type V = ();
    fn status(&self) -> RowStatus<Self::V> {
        match *self {
            HasBlob(true) => RowStatus::Keep(()),
            HasBlob(false) => RowStatus::Delete,
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

impl From<InsertBlob> for (BlobRef, BlobMeta, HasBlob, ValidFrom) {
    fn from(b: InsertBlob) -> Self {
        (
            BlobRef {
                blob_id: b.blob_id,
                repo_id: b.repo_id,
            },
            BlobMeta {
                size: b.blob_size,
                path: b.path,
            },
            HasBlob(b.has_blob),
            ValidFrom {
                valid_from: b.valid_from,
            },
        )
    }
}

impl From<Blob> for (Uid, BlobRef, BlobMeta, HasBlob, ValidFrom) {
    fn from(b: Blob) -> Self {
        (
            b.uid,
            BlobRef {
                blob_id: b.blob_id,
                repo_id: b.repo_id,
            },
            BlobMeta {
                size: b.blob_size,
                path: b.path,
            },
            HasBlob(b.has_blob),
            ValidFrom {
                valid_from: b.valid_from,
            },
        )
    }
}

impl From<(Uid, BlobRef, BlobMeta, HasBlob, ValidFrom)> for Blob {
    fn from((u, br, lb, hb, vf): (Uid, BlobRef, BlobMeta, HasBlob, ValidFrom)) -> Self {
        Self {
            uid: u,
            repo_id: br.repo_id,
            blob_id: br.blob_id,
            blob_size: lb.size,
            has_blob: hb.0,
            path: lb.path,
            valid_from: vf.valid_from,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CurrentFile {
    pub blob_id: BlobID,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct File {
    pub uid: Uid,
    pub path: Path,
    pub blob_id: Option<BlobID>,
    pub valid_from: DateTime<Utc>,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileBlobID(pub Option<BlobID>);

impl Status for FileBlobID {
    type V = BlobID;
    fn status(&self) -> RowStatus<Self::V> {
        match self.clone() {
            FileBlobID(Some(blob_id)) => RowStatus::Keep(blob_id),
            FileBlobID(None) => RowStatus::Delete,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InsertFile {
    pub path: Path,
    pub blob_id: Option<BlobID>,
    pub valid_from: DateTime<Utc>,
}

impl From<InsertFile> for (Path, (), FileBlobID, ValidFrom) {
    fn from(file: InsertFile) -> Self {
        (
            file.path,
            (),
            FileBlobID(file.blob_id),
            ValidFrom {
                valid_from: file.valid_from,
            },
        )
    }
}
impl From<File> for (Uid, Path, (), FileBlobID, ValidFrom) {
    fn from(file: File) -> Self {
        (
            file.uid,
            file.path,
            (),
            FileBlobID(file.blob_id),
            ValidFrom {
                valid_from: file.valid_from,
            },
        )
    }
}

impl From<(Uid, Path, (), FileBlobID, ValidFrom)> for File {
    fn from((uid, path, _, fb, vf): (Uid, Path, (), FileBlobID, ValidFrom)) -> Self {
        Self {
            uid,
            path,
            blob_id: fb.0,
            valid_from: vf.valid_from,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Materialisation {
    pub blob_id: BlobID,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Observation {
    pub fs_last_seen_id: i64,
    pub fs_last_seen_dttm: DateTime<Utc>,
    pub fs_last_modified_dttm: DateTime<Utc>,
    pub fs_last_size: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Check {
    pub check_last_dttm: DateTime<Utc>,
    pub check_last_hash: BlobID,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LocalRepository {
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
pub struct ConnectionName(pub String);

#[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
pub enum ConnectionType {
    Local,
    Ssh,
    RClone,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConnectionMetadata {
    pub connection_type: ConnectionType,
    pub parameter: String,
}
#[derive(Debug, Clone)]
pub struct Connection {
    pub name: ConnectionName,
    pub connection_type: ConnectionType,
    pub parameter: String,
}

#[derive(
    Debug, Clone, Copy, serde::Serialize, serde::Deserialize, Eq, PartialEq, Ord, PartialOrd,
)]
pub struct LogOffset(pub u64);

impl From<LogOffset> for log::Offset {
    fn from(o: LogOffset) -> Self {
        o.0.into()
    }
}

impl From<log::Offset> for LogOffset {
    fn from(o: log::Offset) -> Self {
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
        blob: BlobMeta,
    },
    OkMaterialisationMissing {
        file: CurrentFile,
        #[allow(dead_code)]
        blob: BlobMeta,
    },
    OkBlobMissing {
        file: CurrentFile,
    },
    Altered {
        file: CurrentFile,
        blob: Option<BlobMeta>,
    },
    Outdated {
        file: Option<CurrentFile>,
        blob: Option<BlobMeta>,
        #[allow(dead_code)]
        mat: Materialisation,
    },
    NeedsCheck,
    CorruptionDetected {
        file: CurrentFile,
        #[allow(dead_code)]
        blob: Option<BlobMeta>,
    },
}

#[derive(Debug, Clone)]
pub struct VirtualFile {
    pub file_seen: FileSeen,
    pub current_file: Option<CurrentFile>,
    pub current_blob: Option<BlobMeta>,
    pub current_materialisation: Option<Materialisation>,
    pub current_check: Option<Check>,
}

impl VirtualFile {
    pub fn state(&self) -> VirtualFileState {
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
                    if self.file_seen.size != blob.size {
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
pub struct LogRepositoryName {
    pub name: String,
}

impl From<InsertRepositoryName> for (RepoID, LogRepositoryName, Always, ValidFrom) {
    fn from(rn: InsertRepositoryName) -> Self {
        (
            rn.repo_id,
            LogRepositoryName { name: rn.name },
            Always(),
            ValidFrom {
                valid_from: rn.valid_from,
            },
        )
    }
}
impl From<RepositoryName> for (Uid, RepoID, LogRepositoryName, Always, ValidFrom) {
    fn from(rn: RepositoryName) -> Self {
        (
            rn.uid,
            rn.repo_id,
            LogRepositoryName { name: rn.name },
            Always(),
            ValidFrom {
                valid_from: rn.valid_from,
            },
        )
    }
}

impl From<(Uid, RepoID, LogRepositoryName, Always, ValidFrom)> for RepositoryName {
    fn from(
        (uid, repo_id, name, _, vf): (Uid, RepoID, LogRepositoryName, Always, ValidFrom),
    ) -> Self {
        Self {
            uid,
            repo_id,
            name: name.name,
            valid_from: vf.valid_from,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InsertMaterialisation {
    pub path: Path,
    pub blob_id: Option<BlobID>,
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
pub enum BlobState {
    Present,
    Missing,
}

#[derive(Debug, Clone)]
pub struct FilesWithAvailability {
    pub path: Path,
    #[allow(dead_code)]
    pub blob_id: BlobID,
    pub blob_state: BlobState,
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
