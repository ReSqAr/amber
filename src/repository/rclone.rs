use crate::db::models::{
    AvailableBlob, BlobAssociatedToFiles, CopiedTransferItem, FileTransferItem, InsertBlob,
    InsertRepositoryName, RepoID,
};
use crate::repository::local::LocalRepository;
use crate::repository::traits::{
    Adder, Availability, Metadata, RcloneTargetPath, Receiver, RepositoryMetadata, Sender,
};
use crate::utils::errors::InternalError;
use futures::{FutureExt, StreamExt, TryStreamExt, pin_mut, stream};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use uuid::Uuid;

#[derive(Clone)]
pub struct RCloneStore {
    local: LocalRepository,
    repo_id: RepoID,
    name: String,
    path: String,
}

impl RCloneStore {
    pub async fn new(
        local: &LocalRepository,
        name: &str,
        path: &str,
    ) -> Result<Self, InternalError> {
        let repo_id = RepoID(Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_ref()).to_string());
        local
            .db()
            .add_repository_names(
                stream::iter([InsertRepositoryName {
                    repo_id: repo_id.clone(),
                    name: name.into(),
                    valid_from: chrono::Utc::now(),
                }])
                .boxed(),
            )
            .await?;

        Ok(Self {
            local: local.clone(),
            name: name.into(),
            repo_id,
            path: path.into(),
        })
    }
    pub(crate) async fn close(&self) -> Result<(), InternalError> {
        self.local.close().await?;
        Ok(())
    }
}

impl Metadata for RCloneStore {
    fn current(&self) -> BoxFuture<'_, Result<RepositoryMetadata, InternalError>> {
        let name = self.name.clone();
        Box::pin(async move {
            Ok(RepositoryMetadata {
                id: RepoID(Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_ref()).to_string()),
                name: name.clone(),
            })
        })
    }
}

impl RcloneTargetPath for RCloneStore {
    fn rclone_path(&self, _transfer_id: u32) -> BoxFuture<'_, Result<String, InternalError>> {
        let path = self.path.clone();
        async move { Ok(path) }.boxed()
    }
}

impl Sender<FileTransferItem> for RCloneStore {
    fn prepare_transfer(
        &self,
        s: BoxStream<'static, FileTransferItem>,
    ) -> BoxFuture<'_, Result<u64, InternalError>> {
        let stream = s;
        async move {
            let mut count = 0;
            pin_mut!(stream);
            while tokio_stream::StreamExt::next(&mut stream).await.is_some() {
                count += 1
            }

            Ok(count)
        }
        .boxed()
    }
}

impl Receiver<FileTransferItem> for RCloneStore {
    fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: RepoID,
        paths: Vec<String>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<FileTransferItem, InternalError>>> {
        let db = self.local.db().clone();
        let local_repo_id = self.repo_id.clone();
        async move {
            db.select_missing_files_for_transfer(transfer_id, local_repo_id, repo_id, paths)
                .await
                .err_into()
                .boxed()
        }
        .boxed()
    }

    fn finalise_transfer(
        &self,
        s: BoxStream<'static, CopiedTransferItem>,
    ) -> BoxFuture<'_, Result<u64, InternalError>> {
        let repo_id = self.repo_id.clone();

        async move {
            let s = s
                .map(move |i: CopiedTransferItem| InsertBlob {
                    repo_id: repo_id.clone(),
                    blob_id: i.blob_id,
                    blob_size: i.blob_size,
                    has_blob: true,
                    path: Some(i.path),
                    valid_from: chrono::Utc::now(),
                })
                .boxed();
            Ok(self.local.add_blobs(s).await?)
        }
        .boxed()
    }
}

impl Availability for RCloneStore {
    fn available(&self) -> BoxStream<'static, Result<AvailableBlob, InternalError>> {
        self.local
            .db()
            .available_blobs(self.repo_id.clone())
            .err_into()
            .boxed()
    }

    fn missing(
        &self,
    ) -> BoxFuture<'_, BoxStream<'static, Result<BlobAssociatedToFiles, InternalError>>> {
        let db = self.local.db().clone();
        let repo_id = self.repo_id.clone();
        async move { db.missing_blobs(repo_id).await.err_into().boxed() }.boxed()
    }
}
