use crate::db::models::{
    AvailableBlob, BlobAssociatedToFiles, CopiedTransferItem, FileTransferItem,
    FilesWithAvailability, InsertBlob, InsertRepositoryName, RepoID,
};
use crate::flightdeck::tracer::Tracer;
use crate::repository::local::LocalRepository;
use crate::repository::traits::{
    Adder, Availability, Local, Metadata, RcloneTargetPath, Receiver, RepositoryMetadata, Sender,
};
use crate::utils::errors::InternalError;
use crate::utils::path::RepoPath;
use crate::utils::rclone::{
    ConfigSection, ERROR_CODE_DIRECTORY_NOT_FOUND, Operation, RCloneConfig, RCloneTarget,
    run_rclone,
};
use futures::{FutureExt, StreamExt, TryStreamExt, pin_mut, stream};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use rand::Rng;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

const EXTERNAL_PATH: &str = ".amb";

#[derive(Clone)]
pub struct RCloneStore {
    local: LocalRepository,
    repo_id: RepoID,
    name: String,
    remote_name: String,
    remote_path: String,
}

impl RCloneStore {
    pub async fn connect(
        local: &LocalRepository,
        name: &str,
        remote_name: &str,
        remote_path: &str,
    ) -> Result<Self, InternalError> {
        let transfer_id: u32 = rand::rng().random();
        let staging = local.staging_id_path(transfer_id);
        let files = staging.join("files");

        sync(
            &staging,
            &files,
            remote_name,
            remote_path,
            Direction::Download,
        )
        .await?;

        let external_path = files.join(EXTERNAL_PATH);
        let id_path = external_path.join("id");
        let repo_id = if !tokio::fs::try_exists(&id_path).await.inspect_err(|e| {
            log::error!("RCloneStore::connect: existence check of ID file failed: {e}")
        })? {
            fs::create_dir_all(&external_path).await.inspect_err(|e| {
                log::error!("RCloneStore::connect: create_dir_all meta directory failed: {e}")
            })?;

            let repo_id = Uuid::new_v4().to_string();
            log::debug!("RCloneStore::connect: created new repository ID: {repo_id:?}");

            let mut writer = File::options()
                .create_new(true)
                .write(true)
                .open(&id_path)
                .await
                .inspect_err(|e| log::error!("RCloneStore::connect: open ID file failed: {e}"))?;
            writer.write_all(repo_id.as_bytes()).await?;
            writer.flush().await?;

            sync(
                &staging,
                &files,
                remote_name,
                remote_path,
                Direction::Upload,
            )
            .await?;

            let repo_id = RepoID(repo_id);
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

            repo_id
        } else {
            let repo_id = tokio::fs::read_to_string(&id_path)
                .await
                .inspect_err(|e| log::error!("RCloneStore::connect: read ID file failed: {e}"))?;
            RepoID(repo_id)
        };

        Ok(Self {
            local: local.clone(),
            name: name.into(),
            repo_id,
            remote_name: remote_name.into(),
            remote_path: remote_path.into(),
        })
    }

    pub(crate) async fn close(&self) -> Result<(), InternalError> {
        // yz handle this
        self.local.close().await?;
        Ok(())
    }
}

enum Direction {
    Download,
    Upload,
}

async fn sync(
    staging: &RepoPath,
    files: &RepoPath,
    remote_name: &str,
    remote_path: &str,
    direction: Direction,
) -> Result<(), InternalError> {
    fs::create_dir_all(&staging)
        .await
        .inspect_err(|e| log::error!("RCloneStore::sync: create_dir_all failed: {e}"))?;
    let rclone_files_path = staging.join("rclone.files");
    {
        let mut writer = File::create(&rclone_files_path)
            .await
            .inspect_err(|e| log::error!("RCloneStore::sync: open rclone.files failed: {e}"))?;
        writer.write_all(EXTERNAL_PATH.as_bytes()).await?;
        writer.flush().await?;
    }

    let remote = Target {
        name: Some(remote_name.to_string()),
        path: remote_path.to_string(),
    };
    let local = Target {
        name: None,
        path: files.abs().to_string_lossy().to_string(),
    };

    let (source, destination) = match direction {
        Direction::Download => (remote, local),
        Direction::Upload => (local, remote),
    };

    let config = RCloneConfig {
        transfers: None,
        checkers: None,
    };

    let tracer_copy = Tracer::new_on("RCloneStore::sync::copy");
    let e = run_rclone(
        Operation::Copy,
        staging.abs(),
        rclone_files_path.abs(),
        source,
        destination,
        config,
        |x| {
            log::debug!("RCloneStore::sync: rclone event: {x:?}");
        },
    )
    .await;
    tracer_copy.measure();

    if let Err(e) = e
        && matches!(e, InternalError::RClone(code) if code != ERROR_CODE_DIRECTORY_NOT_FOUND)
    {
        log::error!("RCloneStore::sync: rclone failed: {e}");
        return Err(e);
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct Target {
    pub name: Option<String>,
    pub path: String,
}

impl RCloneTarget for Target {
    fn to_rclone_arg(&self) -> String {
        match &self.name {
            None => self.path.clone(),
            Some(name) => format!("{}:{}", name, self.path),
        }
    }

    fn to_config_section(&self) -> ConfigSection {
        ConfigSection::GlobalConfig
    }
}

impl Metadata for RCloneStore {
    fn current(&self) -> BoxFuture<'_, Result<RepositoryMetadata, InternalError>> {
        let meta = RepositoryMetadata {
            id: self.repo_id.clone(),
            name: self.name.clone(),
        };
        Box::pin(async move { Ok(meta) })
    }
}

impl RcloneTargetPath for RCloneStore {
    fn rclone_path(&self, _transfer_id: u32) -> BoxFuture<'_, Result<String, InternalError>> {
        let path = self.remote_path.clone();
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

    fn current_files_with_availability(
        &self,
    ) -> BoxFuture<'_, BoxStream<'static, Result<FilesWithAvailability, InternalError>>> {
        let db = self.local.db().clone();
        let repo_id = self.repo_id.clone();
        async move {
            db.current_files_with_availability(repo_id)
                .await
                .err_into()
                .boxed()
        }
        .boxed()
    }
}
