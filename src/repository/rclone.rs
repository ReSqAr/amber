use crate::db::models::{FileTransferItem, InsertBlob};
use crate::repository::local::LocalRepository;
use crate::repository::traits::{
    Adder, Metadata, RcloneTargetPath, Receiver, RepositoryMetadata, Sender,
};
use crate::utils::errors::InternalError;
use crate::utils::pipe::TryForwardIntoExt;
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use uuid::Uuid;

#[derive(Clone)]
pub struct RCloneStore {
    local: LocalRepository,
    repo_id: String,
    name: String,
    path: String,
}

impl RCloneStore {
    pub async fn new(
        local: &LocalRepository,
        name: &str,
        path: &str,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            local: local.clone(),
            name: name.into(),
            repo_id: Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_ref()).to_string(),
            path: path.into(),
        })
    }
}

impl Metadata for RCloneStore {
    async fn current(&self) -> Result<RepositoryMetadata, InternalError> {
        Ok(RepositoryMetadata {
            id: Uuid::new_v5(&Uuid::NAMESPACE_OID, self.name.as_ref()).to_string(),
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
        while (tokio_stream::StreamExt::next(&mut stream).await).is_some() {
            count += 1
        }

        Ok(count)
    }
}

impl Receiver<FileTransferItem> for RCloneStore {
    async fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: String,
    ) -> impl Stream<Item = Result<FileTransferItem, InternalError>> + Unpin + Send + 'static {
        self.local
            .db()
            .populate_missing_files_for_transfer(transfer_id, self.repo_id.clone(), repo_id)
            .await
            .err_into()
            .boxed()
    }

    async fn finalise_transfer(&self, transfer_id: u32) -> Result<u64, InternalError> {
        let repo_id = self.repo_id.clone();

        self.local
            .db()
            .select_files_transfer(transfer_id)
            .await
            .map_ok(move |i: FileTransferItem| InsertBlob {
                repo_id: repo_id.clone(),
                blob_id: i.blob_id,
                blob_size: i.blob_size,
                has_blob: true,
                path: Some(i.path),
                valid_from: chrono::Utc::now(),
            })
            .try_forward_into::<_, _, _, _, InternalError>(|s| async {
                self.local.add_blobs(s).await
            })
            .await
    }
}
