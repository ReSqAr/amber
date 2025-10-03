use crate::db::database::DBOutputStream;
use crate::db::error::DBError;
use crate::db::models::{
    AvailableBlob, BlobAssociatedToFiles, Connection, CopiedTransferItem, MissingFile, MoveEvent,
    Observation, PathType, RmEvent, VirtualFile,
};
use crate::utils::errors::InternalError;
use crate::utils::flow::{ExtFlow, Flow};
use crate::utils::fs::Capability;
use crate::utils::path::RepoPath;
use chrono::{DateTime, Utc};
use futures::Stream;
use std::future::Future;

pub trait Local {
    fn root(&self) -> RepoPath;
    fn repository_path(&self) -> RepoPath;
    fn blobs_path(&self) -> RepoPath;
    fn blob_path(&self, blob_id: &str) -> RepoPath;
    fn staging_path(&self) -> RepoPath;
    fn staging_id_path(&self, staging_id: u32) -> RepoPath;
    fn rclone_target_path(&self, staging_id: u32) -> RepoPath;
    fn log_path(&self) -> RepoPath;
    fn capability(&self) -> &Capability;
}

pub struct RepositoryMetadata {
    pub id: String,
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
    AddFilesDBAddBlobsChannelSize,
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
    fn missing(
        &self,
    ) -> impl Stream<Item = Result<BlobAssociatedToFiles, InternalError>> + Unpin + Send + 'static;
}

pub trait Adder {
    fn add_files<S>(&self, s: S) -> impl Future<Output = Result<u64, DBError>> + Send
    where
        S: Stream<Item = crate::db::models::InsertFile> + Unpin + Send;

    fn add_blobs<S>(&self, s: S) -> impl Future<Output = Result<u64, DBError>> + Send
    where
        S: Stream<Item = crate::db::models::InsertBlob> + Unpin + Send;

    fn observe_blobs<S>(&self, s: S) -> impl Future<Output = Result<u64, DBError>> + Send
    where
        S: Stream<Item = crate::db::models::ObservedBlob> + Unpin + Send;

    fn add_repository_names<S>(&self, s: S) -> impl Future<Output = Result<u64, DBError>> + Send
    where
        S: Stream<Item = crate::db::models::InsertRepositoryName> + Unpin + Send;

    fn add_materialisation<S>(&self, s: S) -> impl Future<Output = Result<u64, DBError>> + Send
    where
        S: Stream<Item = crate::db::models::InsertMaterialisation> + Unpin + Send;
}

#[derive(Debug)]
pub struct LastIndices {
    pub file: i32,
    pub blob: i32,
    pub name: i32,
}

pub trait LastIndicesSyncer {
    fn lookup(
        &self,
        repo_id: String,
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
    async fn reset(&self) -> Result<(), DBError>;
    async fn refresh(&self) -> Result<(), DBError>;
    fn cleanup(&self, last_seen_id: i64) -> impl Future<Output = Result<(), DBError>> + Send;

    fn select_missing_files(
        &self,
        last_seen_id: i64,
    ) -> impl Future<Output = DBOutputStream<'static, MissingFile>> + Send;

    async fn add_observations(
        &self,
        input_stream: impl Stream<Item = Flow<Observation>> + Unpin + Send + 'static,
    ) -> impl Stream<Item = ExtFlow<Result<Vec<VirtualFile>, DBError>>> + Unpin + Send + 'static;

    async fn move_files(
        &self,
        src_raw: String,
        dst_raw: String,
        mv_type_hint: PathType,
        now: DateTime<Utc>,
    ) -> impl Stream<Item = Result<MoveEvent, DBError>> + Unpin + Send + 'static;

    async fn remove_files(
        &self,
        files_with_hint: Vec<(String, PathType)>,
        now: DateTime<Utc>,
    ) -> impl Stream<Item = Result<RmEvent, DBError>> + Unpin + Send + 'static;
}

pub trait ConnectionManager {
    async fn add(&self, connection: &Connection) -> Result<(), InternalError>;
    async fn lookup_by_name(&self, name: &str) -> Result<Option<Connection>, InternalError>;
    async fn list(&self) -> Result<Vec<Connection>, InternalError>;
    async fn connect(
        &self,
        name: String,
    ) -> Result<crate::connection::EstablishedConnection, InternalError>;
}

pub trait RcloneTargetPath {
    async fn rclone_path(&self, transfer_id: u32) -> Result<String, InternalError>;
}

pub trait TransferItem: Send + Sync + Clone + 'static {
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
        repo_id: String,
        paths: Vec<String>,
    ) -> impl Future<Output = impl Stream<Item = Result<T, InternalError>> + Unpin + Send + 'static> + Send;

    fn finalise_transfer(
        &self,
        s: impl Stream<Item = CopiedTransferItem> + Unpin + Send + 'static,
    ) -> impl Future<Output = Result<u64, InternalError>> + Send;
}
