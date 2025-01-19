use crate::db::database::DBOutputStream;
use crate::db::models::{BlobId, FilePathWithBlobId, Observation, VirtualFile};
use crate::utils::app_error::AppError;
use futures::Stream;
use std::future::Future;
use std::path::PathBuf;
use crate::utils::control_flow::Message;

pub trait Local {
    fn root(&self) -> PathBuf;
    fn invariable_path(&self) -> PathBuf;
    fn blob_path(&self) -> PathBuf;
}

pub trait Metadata {
    async fn repo_id(&self) -> Result<String, AppError>;
}

pub trait Adder {
    async fn add_files<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = crate::db::models::InsertFile> + Unpin;

    async fn add_blobs<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = crate::db::models::InsertBlob> + Unpin;
}

#[derive(Debug)]
pub struct LastIndices {
    pub file: i32,
    pub blob: i32,
}

pub trait LastIndicesSyncer {
    async fn lookup(&self, repo_id: String) -> Result<LastIndices, AppError>;
    async fn refresh(&self) -> Result<(), AppError>;
}

pub trait SyncerParams {
    type Params;
}

pub trait Syncer<T: SyncerParams> {
    fn select(
        &self,
        params: <T as SyncerParams>::Params,
    ) -> impl Future<Output=impl Stream<Item=Result<T, AppError>> + Unpin + Send + 'static>;

    fn merge<S>(&self, s: S) -> impl Future<Output = Result<(), AppError>> + Send
    where
        S: Stream<Item = T> + Unpin + Send + 'static;
}

pub trait Reconciler {
    fn target_filesystem_state(
        &self
    ) -> impl Stream<Item=Result<FilePathWithBlobId, AppError>> + Unpin + Send;

}

#[deprecated]
pub trait Deprecated {
    #[deprecated]
    fn missing_blobs(
        &self,
        source_repo_id: String,
        target_repo_id: String,
    ) -> impl Stream<Item=Result<BlobId, AppError>> + Unpin + Send;
}


pub trait VirtualFilesystem {
    async fn refresh(&self) -> Result<(), sqlx::Error>;
    async fn cleanup(&self, last_seen_id: i64) -> Result<(), sqlx::Error>;

    async fn select_deleted_files(
        &self,
        last_seen_id: i64,
    ) -> DBOutputStream<'static, VirtualFile>;

    async fn add_observations(
        &self,
        input_stream: impl Stream<Item = Message<Observation>> + Unpin + Send + 'static,
    ) -> impl Stream<Item = Message<Result<VirtualFile, sqlx::Error>>> + Unpin + Send + 'static;
}