use crate::db;
use crate::db::models::InsertBlob;
use crate::grpc::server::invariable::{
    Blob, DownloadRequest, DownloadResponse, File, LookupLastIndicesRequest,
    LookupLastIndicesResponse, MergeBlobsResponse, MergeFilesResponse, MergeRepositoriesResponse,
    Repository, SelectBlobsRequest, SelectFilesRequest, SelectRepositoriesRequest,
    UpdateLastIndicesRequest, UpdateLastIndicesResponse, UploadRequest, UploadResponse,
};
use crate::repository::traits::{Adder, LastIndices, LastIndicesSyncer, Local, Metadata, Syncer};
use crate::utils::app_error::AppError;
use crate::utils::pipe::TryForwardIntoExt;
use anyhow::Context;
use chrono::Utc;
use db::models::Blob as DbBlob;
use db::models::File as DbFile;
use db::models::Repository as DbRepository;
use futures::Stream;
use futures::{stream, TryStreamExt};
use invariable::invariable_server::Invariable;
use invariable::{RepositoryIdRequest, RepositoryIdResponse};
use log::debug;
use std::pin::Pin;
use tokio::fs;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::{Request, Response, Status, Streaming};

pub mod invariable {
    tonic::include_proto!("invariable");
}

pub struct GRPCServer<T> {
    repository: T,
}

impl<T> GRPCServer<T> {
    pub fn new(repository: T) -> Self {
        Self { repository }
    }
}

#[tonic::async_trait]
impl<T> Invariable for GRPCServer<T>
where
    T: Metadata
        + Local
        + Adder
        + LastIndicesSyncer
        + Syncer<DbRepository>
        + Syncer<DbFile>
        + Syncer<DbBlob>
        + Sync
        + Send
        + 'static,
{
    async fn repository_id(
        &self,
        _: Request<RepositoryIdRequest>,
    ) -> Result<Response<RepositoryIdResponse>, Status> {
        let repo_id = self.repository.repo_id().await?;
        Ok(Response::new(RepositoryIdResponse { repo_id }))
    }

    async fn merge_repositories(
        &self,
        request: Request<Streaming<Repository>>,
    ) -> Result<Response<MergeRepositoriesResponse>, Status> {
        request
            .into_inner()
            .map_ok::<DbRepository, _>(Repository::into)
            .try_forward_into::<_, _, _, _, AppError>(|s| self.repository.merge(s))
            .await?;
        Ok(Response::new(MergeRepositoriesResponse {}))
    }
    async fn merge_files(
        &self,
        request: Request<Streaming<File>>,
    ) -> Result<Response<MergeFilesResponse>, Status> {
        request
            .into_inner()
            .map_ok::<DbFile, _>(File::into)
            .try_forward_into::<_, _, _, _, AppError>(|s| self.repository.merge(s))
            .await?;
        Ok(Response::new(MergeFilesResponse {}))
    }
    async fn merge_blobs(
        &self,
        request: Request<Streaming<Blob>>,
    ) -> Result<Response<MergeBlobsResponse>, Status> {
        request
            .into_inner()
            .map_ok::<DbBlob, _>(Blob::into)
            .try_forward_into::<_, _, _, _, AppError>(|s| self.repository.merge(s))
            .await?;
        Ok(Response::new(MergeBlobsResponse {}))
    }

    async fn update_last_indices(
        &self,
        _: Request<UpdateLastIndicesRequest>,
    ) -> Result<Response<UpdateLastIndicesResponse>, Status> {
        self.repository.refresh().await?;
        Ok(Response::new(UpdateLastIndicesResponse {}))
    }

    async fn lookup_last_indices(
        &self,
        request: Request<LookupLastIndicesRequest>,
    ) -> Result<Response<LookupLastIndicesResponse>, Status> {
        let LastIndices { file, blob } =
            self.repository.lookup(request.into_inner().repo_id).await?;
        Ok(Response::new(LookupLastIndicesResponse { file, blob }))
    }

    type SelectRepositoriesStream =
        Pin<Box<dyn Stream<Item = Result<Repository, Status>> + Send + 'static>>;

    async fn select_repositories(
        &self,
        _: Request<SelectRepositoriesRequest>,
    ) -> Result<Response<Self::SelectRepositoriesStream>, Status> {
        let stream = <T as Syncer<DbRepository>>::select(&self.repository, ())
            .await
            .err_into()
            .map_ok::<Repository, _>(DbRepository::into);
        Ok(Response::new(Box::pin(stream)))
    }

    type SelectFilesStream = Pin<Box<dyn Stream<Item = Result<File, Status>> + Send + 'static>>;

    async fn select_files(
        &self,
        request: Request<SelectFilesRequest>,
    ) -> Result<Response<Self::SelectFilesStream>, Status> {
        let last_index = request.into_inner().last_index;
        let stream = <T as Syncer<DbFile>>::select(&self.repository, last_index)
            .await
            .err_into()
            .map_ok::<File, _>(DbFile::into);
        Ok(Response::new(Box::pin(stream)))
    }

    type SelectBlobsStream = Pin<Box<dyn Stream<Item = Result<Blob, Status>> + Send + 'static>>;

    async fn select_blobs(
        &self,
        request: Request<SelectBlobsRequest>,
    ) -> Result<Response<Self::SelectBlobsStream>, Status> {
        let last_index = request.into_inner().last_index;
        let stream = <T as Syncer<DbBlob>>::select(&self.repository, last_index)
            .await
            .err_into()
            .map_ok::<Blob, _>(DbBlob::into);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn upload(
        &self,
        request: Request<UploadRequest>,
    ) -> Result<Response<UploadResponse>, Status> {
        let UploadRequest { blob_id, content } = request.into_inner();

        let blobs_path = self.repository.blobs_path();
        fs::create_dir_all(blobs_path.as_path()).await?;
        let object_path = blobs_path.join(&blob_id);

        let mut file = TokioFile::create(&object_path).await?;
        file.write_all(&content).await?;
        file.sync_all().await?;

        let repo_id = self.repository.repo_id().await?;

        let b = InsertBlob {
            repo_id,
            blob_id,
            has_blob: true,
            blob_size: content.len() as i64,
            valid_from: Utc::now(),
        };
        let sb = stream::iter(vec![b]);
        self.repository
            .add_blobs(sb)
            .await
            .map_err(|err| Status::from_error(err.into()))?;
        debug!("added blob {:?}", object_path);

        Ok(Response::new(UploadResponse {}))
    }

    async fn download(
        &self,
        request: Request<DownloadRequest>,
    ) -> Result<Response<DownloadResponse>, Status> {
        let DownloadRequest { blob_id } = request.into_inner();

        let blobs_path = self.repository.blobs_path().clone();
        let object_path = blobs_path.join(&blob_id);

        let mut file = TokioFile::open(&object_path)
            .await
            .context(format!("unable to access {:?}", object_path))
            .map_err(|err| Status::from_error(err.into()))?;
        let mut content = Vec::new();
        file.read_to_end(&mut content)
            .await
            .context(format!("unable to read {:?}", object_path))
            .map_err(|err| Status::from_error(err.into()))?;
        debug!("read blob {:?}", object_path);

        Ok(Response::new(DownloadResponse { content }))
    }
}
