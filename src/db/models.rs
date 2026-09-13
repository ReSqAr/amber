use crate::db::stores::log;
use crate::db::stores::reduced::{RowStatus, Status, ValidFrom};
use crate::db::versioning::V1;
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
    type V = V1<()>;
    fn status(&self) -> RowStatus<Self::V> {
        match *self {
            HasBlob(true) => RowStatus::Keep(().into()),
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

impl From<InsertBlob> for (BlobRef, V1<BlobMeta>, HasBlob, ValidFrom) {
    fn from(b: InsertBlob) -> Self {
        (
            BlobRef {
                blob_id: b.blob_id,
                repo_id: b.repo_id,
            },
            BlobMeta {
                size: b.blob_size,
                path: b.path,
            }
            .into(),
            HasBlob(b.has_blob),
            ValidFrom {
                valid_from: b.valid_from,
            },
        )
    }
}

impl From<Blob> for (Uid, BlobRef, V1<BlobMeta>, HasBlob, ValidFrom) {
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
            }
            .into(),
            HasBlob(b.has_blob),
            ValidFrom {
                valid_from: b.valid_from,
            },
        )
    }
}

impl From<(Uid, BlobRef, V1<BlobMeta>, HasBlob, ValidFrom)> for Blob {
    fn from((u, br, lb, hb, vf): (Uid, BlobRef, V1<BlobMeta>, HasBlob, ValidFrom)) -> Self {
        let lb: BlobMeta = lb.into_inner();
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
    type V = V1<BlobID>;
    fn status(&self) -> RowStatus<Self::V> {
        match self.clone() {
            FileBlobID(Some(blob_id)) => RowStatus::Keep(blob_id.into()),
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

impl From<InsertFile> for (Path, V1<()>, FileBlobID, ValidFrom) {
    fn from(file: InsertFile) -> Self {
        (
            file.path,
            ().into(),
            FileBlobID(file.blob_id),
            ValidFrom {
                valid_from: file.valid_from,
            },
        )
    }
}
impl From<File> for (Uid, Path, V1<()>, FileBlobID, ValidFrom) {
    fn from(file: File) -> Self {
        (
            file.uid,
            file.path,
            ().into(),
            FileBlobID(file.blob_id),
            ValidFrom {
                valid_from: file.valid_from,
            },
        )
    }
}

impl From<(Uid, Path, V1<()>, FileBlobID, ValidFrom)> for File {
    fn from((uid, path, _, fb, vf): (Uid, Path, V1<()>, FileBlobID, ValidFrom)) -> Self {
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
pub struct SyncState {
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RepositorySyncState {
    pub repo_id: RepoID,
    pub last_file_index: Option<u64>,
    pub last_blob_index: Option<u64>,
    pub last_name_index: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RepositoryMetadata {
    pub uid: Uid,
    pub repo_id: RepoID,
    pub name: Option<String>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct InsertRepositoryMetadata {
    pub repo_id: RepoID,
    pub name: Option<String>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogRepositoryMetadata {
    pub name: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogRepositoryMetadataStatus(pub Option<LogRepositoryMetadata>);

impl Status for LogRepositoryMetadataStatus {
    type V = V1<LogRepositoryMetadata>;

    fn status(&self) -> RowStatus<Self::V> {
        match self.0.clone() {
            Some(value) => RowStatus::Keep(value.into()),
            None => RowStatus::Delete,
        }
    }
}

impl From<InsertRepositoryMetadata> for (RepoID, V1<()>, LogRepositoryMetadataStatus, ValidFrom) {
    fn from(rn: InsertRepositoryMetadata) -> Self {
        (
            rn.repo_id,
            ().into(),
            LogRepositoryMetadataStatus(rn.name.map(|name| LogRepositoryMetadata { name })),
            ValidFrom {
                valid_from: rn.valid_from,
            },
        )
    }
}
impl From<RepositoryMetadata> for (Uid, RepoID, V1<()>, LogRepositoryMetadataStatus, ValidFrom) {
    fn from(rn: RepositoryMetadata) -> Self {
        (
            rn.uid,
            rn.repo_id,
            ().into(),
            LogRepositoryMetadataStatus(rn.name.map(|name| LogRepositoryMetadata { name })),
            ValidFrom {
                valid_from: rn.valid_from,
            },
        )
    }
}

impl From<(Uid, RepoID, V1<()>, LogRepositoryMetadataStatus, ValidFrom)> for RepositoryMetadata {
    fn from(
        (uid, repo_id, _, status, vf): (
            Uid,
            RepoID,
            V1<()>,
            LogRepositoryMetadataStatus,
            ValidFrom,
        ),
    ) -> Self {
        Self {
            uid,
            repo_id,
            name: status.0.map(|value| value.name),
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

// The panic arms below deliberately catch every other state.
#[allow(clippy::wildcard_enum_match_arm)]
#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "aaaa";
    const B: &str = "bbbb";

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).expect("timestamp")
    }

    /// A file as the walker saw it: modified at `t=100`, 10 bytes.
    fn seen(size: u64) -> FileSeen {
        FileSeen {
            path: Path("photo.jpg".into()),
            seen_id: 1,
            seen_dttm: at(200),
            last_modified_dttm: at(100),
            size,
        }
    }

    /// The parts a virtual file is assembled from; every test starts here and
    /// sets only what it is about.
    struct Builder {
        file_seen: FileSeen,
        current_file: Option<CurrentFile>,
        current_blob: Option<BlobMeta>,
        current_materialisation: Option<Materialisation>,
        current_check: Option<Check>,
    }

    impl Builder {
        fn new() -> Self {
            Self {
                file_seen: seen(10),
                current_file: None,
                current_blob: None,
                current_materialisation: None,
                current_check: None,
            }
        }

        fn seen_size(mut self, size: u64) -> Self {
            self.file_seen = seen(size);
            self
        }

        /// The blob the repository wants at this path.
        fn file(mut self, blob_id: &str) -> Self {
            self.current_file = Some(CurrentFile {
                blob_id: BlobID(blob_id.into()),
            });
            self
        }

        /// The blob is available locally, at `size` bytes.
        fn blob(mut self, size: u64) -> Self {
            self.current_blob = Some(BlobMeta { size, path: None });
            self
        }

        /// What was last linked into place at this path.
        fn materialisation(mut self, blob_id: &str) -> Self {
            self.current_materialisation = Some(Materialisation {
                blob_id: BlobID(blob_id.into()),
            });
            self
        }

        /// A hash taken at `t=150`, i.e. after the file was last modified.
        fn check(mut self, blob_id: &str) -> Self {
            self.current_check = Some(Check {
                check_last_dttm: at(150),
                check_last_hash: BlobID(blob_id.into()),
            });
            self
        }

        /// A hash taken at `t=50`, before the file was last modified.
        fn stale_check(mut self, blob_id: &str) -> Self {
            self.current_check = Some(Check {
                check_last_dttm: at(50),
                check_last_hash: BlobID(blob_id.into()),
            });
            self
        }

        fn state(self) -> VirtualFileState {
            VirtualFile {
                file_seen: self.file_seen,
                current_file: self.current_file,
                current_blob: self.current_blob,
                current_materialisation: self.current_materialisation,
                current_check: self.current_check,
            }
            .state()
        }
    }

    #[test]
    fn nothing_known_about_the_file_is_new() {
        assert!(matches!(Builder::new().state(), VirtualFileState::New));
    }

    /// Known to the repository, but never hashed here.
    #[test]
    fn a_file_without_a_check_needs_one() {
        assert!(matches!(
            Builder::new().file(A).blob(10).state(),
            VirtualFileState::NeedsCheck
        ));
    }

    /// The file changed after it was last hashed, so the hash says nothing.
    #[test]
    fn a_check_older_than_the_file_needs_a_new_one() {
        assert!(matches!(
            Builder::new()
                .file(A)
                .blob(10)
                .materialisation(A)
                .stale_check(A)
                .state(),
            VirtualFileState::NeedsCheck
        ));
    }

    #[test]
    fn the_wanted_blob_present_and_materialised_is_ok() {
        assert!(matches!(
            Builder::new()
                .file(A)
                .blob(10)
                .materialisation(A)
                .check(A)
                .state(),
            VirtualFileState::Ok { .. }
        ));
    }

    /// Right content on disk, but the materialisation records another blob.
    #[test]
    fn a_stale_materialisation_record_is_reported() {
        assert!(matches!(
            Builder::new()
                .file(A)
                .blob(10)
                .materialisation(B)
                .check(A)
                .state(),
            VirtualFileState::OkMaterialisationMissing { .. }
        ));
    }

    /// The file is what it should be, but the blob store has lost the blob.
    #[test]
    fn the_wanted_content_without_its_blob_is_ok_but_blob_missing() {
        let state = Builder::new().file(A).materialisation(A).check(A).state();
        match state {
            VirtualFileState::OkBlobMissing { file } => assert_eq!(file.blob_id.0, A),
            other => panic!("expected OkBlobMissing, got {other:?}"),
        }
    }

    /// Hashed to something the repository does not know about.
    #[test]
    fn content_that_matches_neither_file_nor_materialisation_is_altered() {
        let state = Builder::new().file(A).blob(10).check(B).state();
        match state {
            VirtualFileState::Altered { file, blob } => {
                assert_eq!(file.blob_id.0, A);
                assert!(blob.is_some());
            }
            other => panic!("expected Altered, got {other:?}"),
        }
    }

    /// On disk is what was materialised last time; the repository has moved on.
    #[test]
    fn the_previously_materialised_content_is_outdated() {
        let state = Builder::new()
            .file(A)
            .blob(10)
            .materialisation(B)
            .check(B)
            .state();
        match state {
            VirtualFileState::Outdated { file, .. } => {
                assert_eq!(file.expect("file").blob_id.0, A)
            }
            other => panic!("expected Outdated, got {other:?}"),
        }
    }

    /// The same, for a path the repository no longer tracks.
    #[test]
    fn a_materialised_file_deleted_from_the_repository_is_outdated() {
        let state = Builder::new().materialisation(A).check(A).state();
        match state {
            VirtualFileState::Outdated { file, .. } => assert!(file.is_none()),
            other => panic!("expected Outdated, got {other:?}"),
        }
    }

    /// The hash matches the wanted blob but the size does not, so the two
    /// cannot both be true.
    #[test]
    fn a_matching_hash_with_a_different_size_is_corruption() {
        assert!(matches!(
            Builder::new()
                .seen_size(11)
                .file(A)
                .blob(10)
                .materialisation(A)
                .check(A)
                .state(),
            VirtualFileState::CorruptionDetected { .. }
        ));
    }

    /// Untracked path, nothing materialised - but the blob happens to exist
    /// here because some other path uses it.
    #[test]
    fn an_untracked_path_whose_blob_exists_is_new() {
        assert!(matches!(
            Builder::new().blob(10).check(A).state(),
            VirtualFileState::New
        ));
    }
}
