use crate::db::models::{
    AvailableBlob, BlobAssociatedToFiles, CopiedTransferItem, FileTransferItem, InsertBlob,
    InsertRepositoryName, RepoID,
};
use crate::repository::local::LocalRepository;
use crate::repository::traits::{
    Adder, Availability, Metadata, RcloneTargetPath, Receiver, RepositoryMetadata, Sender,
};
use crate::utils::errors::InternalError;
use futures::{Stream, StreamExt, TryStreamExt, pin_mut, stream};
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
            .add_repository_names(stream::iter([InsertRepositoryName {
                repo_id: repo_id.clone(),
                name: name.into(),
                valid_from: chrono::Utc::now(),
            }]))
            .await?;

        Ok(Self {
            local: local.clone(),
            name: name.into(),
            repo_id,
            path: path.into(),
        })
    }
}

impl Metadata for RCloneStore {
    async fn current(&self) -> Result<RepositoryMetadata, InternalError> {
        Ok(RepositoryMetadata {
            id: RepoID(Uuid::new_v5(&Uuid::NAMESPACE_OID, self.name.as_ref()).to_string()),
            name: self.name.clone(),
        })
    }
}

impl RcloneTargetPath for RCloneStore {
    async fn rclone_path(&self, _transfer_id: u32) -> Result<String, InternalError> {
        Ok(self.path.clone())
    }
}

impl Sender<FileTransferItem> for RCloneStore {
    async fn prepare_transfer<S>(&self, s: S) -> Result<u64, InternalError>
    where
        S: Stream<Item = FileTransferItem> + Unpin + Send + 'static,
    {
        let stream = s;
        let mut count = 0;
        pin_mut!(stream);
        while tokio_stream::StreamExt::next(&mut stream).await.is_some() {
            count += 1
        }

        Ok(count)
    }
}

impl Receiver<FileTransferItem> for RCloneStore {
    async fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: RepoID,
        paths: Vec<String>,
    ) -> impl Stream<Item = Result<FileTransferItem, InternalError>> + Unpin + Send + 'static {
        self.local
            .db()
            .select_missing_files_for_transfer(transfer_id, self.repo_id.clone(), repo_id, paths)
            .await
            .err_into()
            .boxed()
    }

    async fn finalise_transfer(
        &self,
        s: impl Stream<Item = CopiedTransferItem> + Unpin + Send + 'static,
    ) -> Result<u64, InternalError> {
        let repo_id = self.repo_id.clone();

        let s = s.map(move |i: CopiedTransferItem| InsertBlob {
            repo_id: repo_id.clone(),
            blob_id: i.blob_id,
            blob_size: i.blob_size,
            has_blob: true,
            path: Some(i.path),
            valid_from: chrono::Utc::now(),
        });
        Ok(self.local.add_blobs(s).await?)
    }
}

impl Availability for RCloneStore {
    fn available(
        &self,
    ) -> impl Stream<Item = Result<AvailableBlob, InternalError>> + Unpin + Send + 'static {
        self.local
            .db()
            .available_blobs(self.repo_id.clone())
            .err_into()
    }

    async fn missing(
        &self,
    ) -> impl Stream<Item = Result<BlobAssociatedToFiles, InternalError>> + Unpin + Send + 'static
    {
        self.local
            .db()
            .missing_blobs(self.repo_id.clone())
            .await
            .err_into()
    }
}
