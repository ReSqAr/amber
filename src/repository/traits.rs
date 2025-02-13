use crate::db::database::DBOutputStream;
use crate::db::models::{BlobWithPaths, Connection, Observation, TransferItem, VirtualFile};
use crate::utils::errors::InternalError;
use crate::utils::flow::{ExtFlow, Flow};
use crate::utils::path::RepoPath;
use futures::Stream;
use std::future::Future;

pub trait Local {
    fn root(&self) -> RepoPath;
    fn repository_path(&self) -> RepoPath;
    fn blobs_path(&self) -> RepoPath;
    fn blob_path(&self, blob_id: String) -> RepoPath;
    fn staging_path(&self) -> RepoPath;
    fn transfer_path(&self, transfer_id: u32) -> RepoPath;
    fn log_path(&self) -> RepoPath;
}

pub trait Metadata {
    fn repo_id(&self) -> impl Future<Output = Result<String, InternalError>> + Send;
}

pub enum BufferType {
    Assimilate,
    TransferRcloneFilesWriter,
    TransferRcloneFilesStream,
    AddFilesBlobifyFutureFileBuffer,
    AddFilesDBAddFiles,
    AddFilesDBAddBlobs,
    AddFilesDBAddMaterialisations,
    PrepareTransfer,
    State,
    Walker,
    StateChecker,
    Materialise,
}
pub trait Config {
    fn buffer_size(&self, buffer: BufferType) -> usize;
}

pub trait Missing {
    fn missing(&self) -> impl Stream<Item = Result<BlobWithPaths, InternalError>> + Unpin + Send;
}

pub trait Adder {
    fn add_files<S>(&self, s: S) -> impl Future<Output = Result<u64, sqlx::Error>> + Send
    where
        S: Stream<Item = crate::db::models::InsertFile> + Unpin + Send;

    fn add_blobs<S>(&self, s: S) -> impl Future<Output = Result<u64, sqlx::Error>> + Send
    where
        S: Stream<Item = crate::db::models::InsertBlob> + Unpin + Send;

    fn add_materialisation<S>(&self, s: S) -> impl Future<Output = Result<u64, sqlx::Error>> + Send
    where
        S: Stream<Item = crate::db::models::InsertMaterialisation> + Unpin + Send;
}

#[derive(Debug)]
pub struct LastIndices {
    pub file: i32,
    pub blob: i32,
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
    async fn refresh(&self) -> Result<(), sqlx::Error>;
    fn cleanup(&self, last_seen_id: i64) -> impl Future<Output = Result<(), sqlx::Error>> + Send;

    fn select_deleted_files(
        &self,
        last_seen_id: i64,
    ) -> impl Future<Output = DBOutputStream<'static, VirtualFile>> + Send;

    async fn add_observations(
        &self,
        input_stream: impl Stream<Item = Flow<Observation>> + Unpin + Send + 'static,
    ) -> impl Stream<Item = ExtFlow<Result<Vec<VirtualFile>, sqlx::Error>>> + Unpin + Send + 'static;
}

pub trait ConnectionManager {
    async fn add(&self, connection: &Connection) -> Result<(), InternalError>;
    async fn lookup_by_name(&self, name: &str) -> Result<Option<Connection>, InternalError>;
    async fn list(&self) -> Result<Vec<Connection>, InternalError>;
    async fn connect(
        &self,
        name: String,
    ) -> Result<crate::repository::connection::EstablishedConnection, InternalError>;
}

pub trait BlobSender {
    fn prepare_transfer<S>(&self, s: S) -> impl Future<Output = Result<u64, InternalError>> + Send
    where
        S: Stream<Item = TransferItem> + Unpin + Send + 'static;
}

pub trait BlobReceiver {
    fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: String,
    ) -> impl Future<
        Output = impl Stream<Item = Result<TransferItem, InternalError>> + Unpin + Send + 'static,
    > + Send;

    fn finalise_transfer(
        &self,
        transfer_id: u32,
    ) -> impl Future<Output = Result<u64, InternalError>> + Send;
}
