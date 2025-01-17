use futures::{Stream, StreamExt};
use crate::db;
use crate::db::db::DB;
use crate::db::models::CurrentRepository;
use crate::transport::server::invariable::{
    Blob, DownloadRequest, DownloadResponse, File, LookupRepositoryRequest,
    LookupRepositoryResponse, MergeBlobsResponse, MergeFilesResponse, MergeRepositoriesResponse,
    Repository, SelectBlobsRequest, SelectFilesRequest, SelectRepositoriesRequest,
    UpdateLastIndicesRequest, UpdateLastIndicesResponse, UploadRequest, UploadResponse,
};
use chrono::{DateTime, TimeZone, Utc};
use db::models::Blob as DbBlob;
use db::models::File as DbFile;
use db::models::Repository as DbRepository;
use futures::{TryStreamExt};
use invariable::invariable_server::Invariable;
use invariable::{RepositoryIdRequest, RepositoryIdResponse};
use log::debug;
use prost_types::Timestamp;
use std::path::PathBuf;
use std::pin::Pin;
use anyhow::Context;
use tokio::fs;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

pub mod invariable {
    tonic::include_proto!("invariable");
}

fn datetime_to_timestamp(dt: &DateTime<Utc>) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    })
}

fn timestamp_to_datetime(ts: &Option<Timestamp>) -> DateTime<Utc> {
    ts.and_then(|ts| Utc.timestamp_opt(ts.seconds, ts.nanos as u32).single())
        .unwrap_or_default()
}

impl From<DbRepository> for Repository {
    fn from(repo: DbRepository) -> Self {
        Repository {
            repo_id: repo.repo_id,
            last_file_index: repo.last_file_index,
            last_blob_index: repo.last_blob_index,
        }
    }
}

impl From<DbFile> for File {
    fn from(file: DbFile) -> Self {
        File {
            uuid: file.uuid,
            path: file.path,
            object_id: file.object_id,
            valid_from: datetime_to_timestamp(&file.valid_from),
        }
    }
}

impl From<DbBlob> for Blob {
    fn from(blob: DbBlob) -> Self {
        Blob {
            uuid: blob.uuid,
            repo_id: blob.repo_id,
            object_id: blob.object_id,
            file_exists: blob.file_exists,
            valid_from: datetime_to_timestamp(&blob.valid_from),
        }
    }
}

impl From<Repository> for DbRepository {
    fn from(repo: Repository) -> Self {
        DbRepository {
            repo_id: repo.repo_id,
            last_file_index: repo.last_file_index,
            last_blob_index: repo.last_blob_index,
        }
    }
}
impl From<File> for DbFile {
    fn from(file: File) -> Self {
        DbFile {
            uuid: file.uuid,
            path: file.path,
            object_id: file.object_id,
            valid_from: timestamp_to_datetime(&file.valid_from),
        }
    }
}

// Conversion from Blob to DbBlob
impl From<Blob> for DbBlob {
    fn from(blob: Blob) -> Self {
        DbBlob {
            uuid: blob.uuid,
            repo_id: blob.repo_id,
            object_id: blob.object_id,
            file_exists: blob.file_exists,
            valid_from: timestamp_to_datetime(&blob.valid_from),
        }
    }
}

pub struct MyServer {
    pub(crate) db: &'static DB,
    pub invariable_path: PathBuf,
}

#[tonic::async_trait]
impl Invariable for MyServer {
    async fn repository_id(
        &self,
        _: Request<RepositoryIdRequest>,
    ) -> Result<Response<RepositoryIdResponse>, Status> {
        match self.db.get_or_create_current_repository().await {
            Ok(CurrentRepository { repo_id }) => {
                Ok(Response::new(RepositoryIdResponse { repo_id }))
            }
            Err(err) => Err(Status::from_error(err.into())),
        }
    }

    async fn merge_repositories(
        &self,
        request: Request<Streaming<Repository>>,
    ) -> Result<Response<MergeRepositoriesResponse>, Status> {
        let repos = request
            .into_inner()
            .map_ok(Repository::into)
            .try_collect()
            .await?;
        match self.db.merge_repositories(repos).await {
            Ok(_) => Ok(Response::new(MergeRepositoriesResponse {})),
            Err(err) => Err(Status::from_error(err.into())),
        }
    }
    async fn merge_files(
        &self,
        request: Request<Streaming<File>>,
    ) -> Result<Response<MergeFilesResponse>, Status> {
        let files = request
            .into_inner()
            .map_ok(File::into)
            .try_collect()
            .await?;
        match self.db.merge_files(files).await {
            Ok(_) => Ok(Response::new(MergeFilesResponse {})),
            Err(err) => Err(Status::from_error(err.into())),
        }
    }
    async fn merge_blobs(
        &self,
        request: Request<Streaming<Blob>>,
    ) -> Result<Response<MergeBlobsResponse>, Status> {
        let blobs = request
            .into_inner()
            .map_ok(Blob::into)
            .try_collect()
            .await?;
        match self.db.merge_blobs(blobs).await {
            Ok(_) => Ok(Response::new(MergeBlobsResponse {})),
            Err(err) => Err(Status::from_error(err.into())),
        }
    }

    async fn update_last_indices(
        &self,
        _: Request<UpdateLastIndicesRequest>,
    ) -> Result<Response<UpdateLastIndicesResponse>, Status> {
        match self.db.update_last_indices().await {
            Ok(repo) => Ok(Response::new(UpdateLastIndicesResponse {
                current: Some(repo.into()),
            })),
            Err(err) => Err(Status::from_error(err.into())),
        }
    }

    async fn lookup_repository(
        &self,
        request: Request<LookupRepositoryRequest>,
    ) -> Result<Response<LookupRepositoryResponse>, Status> {
        match self
            .db
            .lookup_repository(request.into_inner().repo_id)
            .await
        {
            Ok(repo) => Ok(Response::new(LookupRepositoryResponse {
                repo: Some(repo.into()),
            })),
            Err(err) => Err(Status::from_error(err.into())),
        }
    }

    type SelectRepositoriesStream = ReceiverStream<Result<Repository, Status>>;

    async fn select_repositories(
        &self,
        _: Request<SelectRepositoriesRequest>,
    ) -> Result<Response<Self::SelectRepositoriesStream>, Status> {
        match self.db.select_repositories().await {
            Ok(repos) => {
                let (tx, rx) = mpsc::channel(1000);
                tokio::spawn(async move {
                    for repo in repos {
                        if tx.send(Ok(repo.into())).await.is_err() {
                            break;
                        }
                    }
                });

                Ok(Response::new(ReceiverStream::new(rx)))
            }
            Err(err) => Err(Status::from_error(err.into())),
        }
    }

    type SelectFilesStream = Pin<Box<dyn Stream<Item = Result<File, Status>> + Send + 'static>>;

    async fn select_files(
        &self,
        request: Request<SelectFilesRequest>,
    ) -> Result<Response<Self::SelectFilesStream>, Status> {
        let last_index = request.into_inner().last_index;
        let stream = self.db.select_files(last_index);
        let mapped_stream = stream.map(|r| match r {
            Ok(file) => Ok(file.into()),
            Err(err) => Err(Status::from_error(err.into())),
        });
        Ok(Response::new(Box::pin(mapped_stream)))
    }

    type SelectBlobsStream = ReceiverStream<Result<Blob, Status>>;

    async fn select_blobs(
        &self,
        request: Request<SelectBlobsRequest>,
    ) -> Result<Response<Self::SelectBlobsStream>, Status> {
        match self.db.select_blobs(&request.into_inner().last_index).await {
            Ok(blobs) => {
                let (tx, rx) = mpsc::channel(1000);
                tokio::spawn(async move {
                    for blob in blobs {
                        if tx.send(Ok(blob.into())).await.is_err() {
                            break;
                        }
                    }
                });

                Ok(Response::new(ReceiverStream::new(rx)))
            }
            Err(err) => Err(Status::from_error(err.into())),
        }
    }

    async fn upload(
        &self,
        request: Request<UploadRequest>,
    ) -> Result<Response<UploadResponse>, Status> {
        let UploadRequest { object_id, content } = request.into_inner();

        let blob_path = self.invariable_path.join("blobs");
        fs::create_dir_all(blob_path.as_path()).await?;
        let object_path = blob_path.join(&object_id);

        let mut file = TokioFile::create(&object_path).await?;
        file.write_all(&content).await?;
        file.sync_all().await?;

        let CurrentRepository { repo_id } = self
            .db
            .get_or_create_current_repository()
            .await
            .or_else(|err| Err(Status::from_error(err.into())))?;
        self.db
            .add_blob(&repo_id, &object_id, chrono::Utc::now(), true)
            .await
            .or_else(|err| Err(Status::from_error(err.into())))?;
        debug!("added blob {:?}", object_path);

        Ok(Response::new(UploadResponse {}))
    }

    async fn download(
        &self,
        request: Request<DownloadRequest>,
    ) -> Result<Response<DownloadResponse>, Status> {
        let DownloadRequest { object_id } = request.into_inner();

        let blob_path = self.invariable_path.join("blobs");
        let object_path = blob_path.join(&object_id);

        let mut file = TokioFile::open(&object_path).await.context(format!("unable to access {:?}", object_path)).or_else(|err| Err(Status::from_error(err.into())))?;
        let mut content = Vec::new();
        file.read_to_end(&mut content).await.context(format!("unable to read {:?}", object_path)).or_else(|err| Err(Status::from_error(err.into())))?;
        debug!("read blob {:?}", object_path);

        Ok(Response::new(DownloadResponse { content }))
    }
}
