use crate::db;
use crate::db::database::DBOutputStream;
use crate::db::error::DBError;
use crate::db::models;
use crate::db::models::{
    AvailableBlob, BlobAssociatedToFiles, BlobID, Connection, ConnectionName, CopiedTransferItem,
    CurrentFile, FileCheck, FileSeen, InsertFileBundle, MissingFile, RepoID, VirtualFile,
};
use crate::utils::errors::InternalError;
use crate::utils::fs::Capability;
use crate::utils::path::RepoPath;
use futures::Stream;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

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
    fn current(&self) -> impl Future<Output = Result<RepositoryMetadata, InternalError>> + Send;
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
    fn available(
        &self,
    ) -> impl Stream<Item = Result<AvailableBlob, InternalError>> + Unpin + Send + 'static;
    async fn missing(
        &self,
    ) -> impl Stream<Item = Result<BlobAssociatedToFiles, InternalError>> + Unpin + Send + 'static;
}

pub trait Adder {
    #[allow(dead_code)]
    fn add_files<S>(&self, s: S) -> impl Future<Output = Result<u64, DBError>> + Send
    where
        S: Stream<Item = crate::db::models::InsertFile> + Unpin + Send;

    fn add_blobs<S>(&self, s: S) -> impl Future<Output = Result<u64, DBError>> + Send
    where
        S: Stream<Item = crate::db::models::InsertBlob> + Unpin + Send;

    fn add_file_bundles<S>(&self, s: S) -> impl Future<Output = Result<u64, DBError>> + Send
    where
        S: Stream<Item = InsertFileBundle> + Unpin + Send;

    fn add_repository_names<S>(&self, s: S) -> impl Future<Output = Result<u64, DBError>> + Send
    where
        S: Stream<Item = crate::db::models::InsertRepositoryName> + Unpin + Send;

    fn add_materialisation<S>(&self, s: S) -> impl Future<Output = Result<u64, DBError>> + Send
    where
        S: Stream<Item = crate::db::models::InsertMaterialisation> + Unpin + Send;
}

#[derive(Debug)]
pub struct LastIndices {
    pub file: Option<u64>,
    pub blob: Option<u64>,
    pub name: Option<u64>,
}

pub trait LastIndicesSyncer {
    fn lookup(
        &self,
        repo_id: RepoID,
    ) -> impl Future<Output = Result<LastIndices, InternalError>> + Send;
    fn refresh(&self) -> impl Future<Output = Result<(), InternalError>> + Send;
}

pub trait SyncerParams {
    type Params;
}

pub trait Syncer<T: SyncerParams> {
    fn select(
        &self,
        params: <T as SyncerParams>::Params,
    ) -> impl Future<Output = impl Stream<Item = Result<T, InternalError>> + Unpin + Send + 'static> + Send;

    fn merge<S>(&self, s: S) -> impl Future<Output = Result<(), InternalError>> + Send
    where
        S: Stream<Item = T> + Unpin + Send + 'static;
}

pub trait VirtualFilesystem {
    fn select_missing_files(
        &self,
        last_seen_id: i64,
    ) -> impl Future<Output = DBOutputStream<'static, MissingFile>> + Send;

    fn add_checked_events(
        &self,
        s: impl Stream<Item = FileCheck> + Unpin + Send + 'static,
    ) -> Pin<Box<dyn Future<Output = Result<u64, DBError>> + Send>>;

    fn add_seen_events(
        &self,
        s: impl Stream<Item = FileSeen> + Unpin + Send + 'static,
    ) -> Pin<Box<dyn Future<Output = Result<u64, DBError>> + Send>>;

    fn select_virtual_filesystem(
        &self,
        s: impl Stream<Item = FileSeen> + Unpin + Send + 'static,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = impl Stream<Item = Result<VirtualFile, DBError>> + Unpin + Send + 'static,
                > + Send,
        >,
    >;
    async fn select_current_files(
        &self,
        file_or_dir: String,
    ) -> impl Stream<Item = Result<(models::Path, BlobID), DBError>> + Unpin + Send + 'static;

    fn left_join_current_files<
        K: Clone + Send + Sync + 'static,
        E: From<DBError> + Debug + Send + Sync + 'static,
    >(
        &self,
        s: impl Stream<Item = Result<K, E>> + Unpin + Send + 'static,
        key_func: impl Fn(K) -> db::models::Path + Sync + Send + 'static,
    ) -> impl Stream<Item = Result<(K, Option<CurrentFile>), E>> + Unpin + Send + 'static;
}

pub trait ConnectionManager {
    async fn add(&self, connection: Connection) -> Result<(), InternalError>;
    async fn lookup_by_name(
        &self,
        name: ConnectionName,
    ) -> Result<Option<Connection>, InternalError>;
    async fn list(&self) -> Result<Vec<Connection>, InternalError>;
    async fn connect(
        &self,
        name: ConnectionName,
    ) -> Result<crate::connection::EstablishedConnection, InternalError>;
}

pub trait RcloneTargetPath {
    async fn rclone_path(&self, transfer_id: u32) -> Result<String, InternalError>;
}

pub trait TransferItem: Send + Sync + Clone + Into<models::SizedBlobID> + 'static {
    fn new(path: models::Path, transfer_id: u32, sized: models::SizedBlobID) -> Self;
    fn path(&self) -> String;
}

pub trait Sender<T: TransferItem> {
    fn prepare_transfer<S>(&self, s: S) -> impl Future<Output = Result<u64, InternalError>> + Send
    where
        S: Stream<Item = T> + Unpin + Send + 'static;
}

pub trait Receiver<T: TransferItem> {
    fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: RepoID,
        paths: Vec<String>,
    ) -> impl Future<Output = impl Stream<Item = Result<T, InternalError>> + Unpin + Send + 'static> + Send;

    fn finalise_transfer(
        &self,
        s: impl Stream<Item = CopiedTransferItem> + Unpin + Send + 'static,
    ) -> impl Future<Output = Result<u64, InternalError>> + Send;
}
