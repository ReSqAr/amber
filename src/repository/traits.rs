use crate::db::database::DBOutputStream;
use crate::db::models::{
    BlobId, BlobWithPaths, Connection, FilePathWithBlobId, Observation, VirtualFile,
};
use crate::utils::app_error::AppError;
use crate::utils::flow::{ExtFlow, Flow};
use futures::Stream;
use std::future::Future;
use std::path::PathBuf;

pub trait Local {
    fn root(&self) -> PathBuf;
    fn repository_path(&self) -> PathBuf;
    fn blobs_path(&self) -> PathBuf;
    fn blob_path(&self, blob_id: String) -> PathBuf;
    fn staging_path(&self) -> PathBuf;
}

pub trait Metadata {
    fn repo_id(&self) -> impl Future<Output = Result<String, AppError>> + Send;
}

pub trait Missing {
    fn missing(&self) -> impl Stream<Item = Result<BlobWithPaths, AppError>> + Unpin + Send;
}

pub trait Adder {
    fn add_files<S>(&self, s: S) -> impl Future<Output = Result<(), sqlx::Error>> + Send
    where
        S: Stream<Item = crate::db::models::InsertFile> + Unpin + Send + Sync;

    fn add_blobs<S>(&self, s: S) -> impl Future<Output = Result<(), sqlx::Error>> + Send
    where
        S: Stream<Item = crate::db::models::InsertBlob> + Unpin + Send + Sync;
}

#[derive(Debug)]
pub struct LastIndices {
    pub file: i32,
    pub blob: i32,
}

pub trait LastIndicesSyncer {
    fn lookup(&self, repo_id: String)
        -> impl Future<Output = Result<LastIndices, AppError>> + Send;
    fn refresh(&self) -> impl Future<Output = Result<(), AppError>> + Send;
}

pub trait SyncerParams {
    type Params;
}

pub trait Syncer<T: SyncerParams> {
    fn select(
        &self,
        params: <T as SyncerParams>::Params,
    ) -> impl Future<Output = impl Stream<Item = Result<T, AppError>> + Unpin + Send + 'static> + Send;

    fn merge<S>(&self, s: S) -> impl Future<Output = Result<(), AppError>> + Send
    where
        S: Stream<Item = T> + Unpin + Send + 'static;
}

pub trait Reconciler {
    fn target_filesystem_state(
        &self,
    ) -> impl Stream<Item = Result<FilePathWithBlobId, AppError>> + Unpin + Send;
}

//#[deprecated]
pub trait Deprecated {
    //#[deprecated]
    fn deprecated_missing_blobs(
        &self,
        source_repo_id: String,
        target_repo_id: String,
    ) -> impl Stream<Item = Result<BlobId, AppError>> + Unpin + Send;
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
    async fn add(&self, connection: &Connection) -> Result<(), AppError>;
    async fn lookup_by_name(&self, name: &str) -> Result<Option<Connection>, AppError>;
    async fn list(&self) -> Result<Vec<Connection>, AppError>;
    async fn connect(
        &self,
        name: String,
    ) -> Result<crate::repository::connection::Connection, Box<dyn std::error::Error>>;
}
