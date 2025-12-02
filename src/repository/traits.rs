use crate::db::error::DBError;
use crate::db::models;
use crate::db::models::{
    AvailableBlob, BlobAssociatedToFiles, BlobID, Connection, ConnectionName, CopiedTransferItem,
    CurrentFile, FileCheck, FileSeen, InsertFileBundle, MissingFile, RepoID, VirtualFile,
};
use crate::utils::errors::InternalError;
use crate::utils::fs::Capability;
use crate::utils::path::RepoPath;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use std::fmt::Debug;

pub trait Local {
    fn root(&self) -> RepoPath;
    fn repository_path(&self) -> RepoPath;
    fn blobs_path(&self) -> RepoPath;
    fn blob_path(&self, blob_id: &BlobID) -> RepoPath;
    fn staging_path(&self) -> RepoPath;
    fn staging_id_path(&self, staging_id: u32) -> RepoPath;
    fn rclone_target_path(&self, staging_id: u32) -> RepoPath;
    fn log_path(&self) -> RepoPath;
    fn capability(&self) -> &Capability;
}

pub struct RepositoryMetadata {
    pub id: RepoID,
    pub name: String,
}

pub trait Metadata {
    fn current(&self) -> BoxFuture<'_, Result<RepositoryMetadata, InternalError>>;
}

pub enum BufferType {
    AssimilateParallelism,
    TransferRcloneFilesWriterChunkSize,
    TransferRcloneFilesStreamChunkSize,
    AddFilesBlobifyFutureFileParallelism,
    AddFilesDBAddFilesChannelSize,
    AddFilesDBAddMaterialisationsChannelSize,
    PrepareTransferParallelism,
    StateBufferChannelSize,
    WalkerChannelSize,
    StateCheckerParallelism,
    MaterialiseParallelism,
    FsckBufferParallelism,
    FsckMaterialiseBufferParallelism,
    FsckRcloneFilesWriterChannelSize,
    FsckRcloneFilesStreamChunkSize,
    FsMvParallelism,
    FsRmParallelism,
}
pub trait Config {
    fn buffer_size(&self, buffer: BufferType) -> usize;
}

pub trait Availability {
    fn available(&self) -> BoxStream<'static, Result<AvailableBlob, InternalError>>;
    fn missing(
        &self,
    ) -> BoxFuture<'_, BoxStream<'static, Result<BlobAssociatedToFiles, InternalError>>>;
}

pub trait Adder {
    #[allow(dead_code)]
    fn add_files<'a>(
        &'a self,
        s: BoxStream<'a, models::InsertFile>,
    ) -> BoxFuture<'a, Result<u64, DBError>>;

    fn add_blobs<'a>(
        &'a self,
        s: BoxStream<'a, models::InsertBlob>,
    ) -> BoxFuture<'a, Result<u64, DBError>>;

    fn add_file_bundles<'a>(
        &'a self,
        s: BoxStream<'a, InsertFileBundle>,
    ) -> BoxFuture<'a, Result<u64, DBError>>;

    fn add_repository_names<'a>(
        &'a self,
        s: BoxStream<'a, models::InsertRepositoryName>,
    ) -> BoxFuture<'a, Result<u64, DBError>>;

    fn add_materialisation<'a>(
        &'a self,
        s: BoxStream<'a, models::InsertMaterialisation>,
    ) -> BoxFuture<'a, Result<u64, DBError>>;
}

#[derive(Debug)]
pub struct LastIndices {
    pub file: Option<u64>,
    pub blob: Option<u64>,
    pub name: Option<u64>,
}

pub trait LastIndicesSyncer {
    fn lookup(&self, repo_id: RepoID) -> BoxFuture<'_, Result<LastIndices, InternalError>>;
    fn refresh(&self) -> BoxFuture<'_, Result<(), InternalError>>;
}

pub trait SyncerParams {
    type Params;
}

pub trait Syncer<T: SyncerParams> {
    fn select(
        &self,
        params: <T as SyncerParams>::Params,
    ) -> BoxFuture<'_, BoxStream<'static, Result<T, InternalError>>>;

    fn merge(&self, s: BoxStream<'static, T>) -> BoxFuture<'_, Result<(), InternalError>>;
}

pub trait VirtualFilesystem {
    fn select_missing_files(
        &self,
        last_seen_id: i64,
    ) -> BoxFuture<'_, BoxStream<'static, Result<MissingFile, DBError>>>;

    fn add_checked_events(
        &self,
        s: BoxStream<'static, FileCheck>,
    ) -> BoxFuture<'_, Result<u64, DBError>>;

    fn add_seen_events(
        &self,
        s: BoxStream<'static, FileSeen>,
    ) -> BoxFuture<'_, Result<u64, DBError>>;

    fn select_virtual_filesystem(
        &'_ self,
        s: BoxStream<'static, FileSeen>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<VirtualFile, DBError>>>;
    fn select_current_files(
        &self,
        file_or_dir: String,
    ) -> BoxFuture<'_, BoxStream<'static, Result<(models::Path, BlobID), DBError>>>;

    fn left_join_current_files<
        K: Clone + Send + Sync + 'static,
        E: From<DBError> + Debug + Send + Sync + 'static,
    >(
        &self,
        s: BoxStream<'static, Result<K, E>>,
        key_func: impl Fn(K) -> models::Path + Sync + Send + 'static,
    ) -> BoxStream<'static, Result<(K, Option<CurrentFile>), E>>;
}

pub trait ConnectionManager {
    fn add(&self, connection: Connection) -> BoxFuture<'_, Result<(), InternalError>>;
    fn lookup_by_name(
        &self,
        name: ConnectionName,
    ) -> BoxFuture<'_, Result<Option<Connection>, InternalError>>;
    fn list(&self) -> BoxFuture<'_, Result<Vec<Connection>, InternalError>>;
    fn connect(
        &self,
        name: ConnectionName,
    ) -> BoxFuture<'_, Result<crate::connection::EstablishedConnection, InternalError>>;
}

pub trait RcloneTargetPath {
    fn rclone_path(&self, transfer_id: u32) -> BoxFuture<'_, Result<String, InternalError>>;
}

pub trait TransferItem: Send + Sync + Clone + Into<models::SizedBlobID> + 'static {
    fn new(path: models::Path, transfer_id: u32, sized: models::SizedBlobID) -> Self;
    fn path(&self) -> String;
}

pub trait Sender<T: TransferItem> {
    fn prepare_transfer(
        &self,
        s: BoxStream<'static, T>,
    ) -> BoxFuture<'_, Result<u64, InternalError>>;
}

pub trait Receiver<T: TransferItem> {
    fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: RepoID,
        paths: Vec<String>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<T, InternalError>>>;

    fn finalise_transfer(
        &self,
        s: BoxStream<'static, CopiedTransferItem>,
    ) -> BoxFuture<'_, Result<u64, InternalError>>;
}
