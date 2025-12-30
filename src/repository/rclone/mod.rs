use crate::db::models;
use crate::db::models::{
    AvailableBlob, BlobAssociatedToFiles, CopiedTransferItem, FileTransferItem,
    FilesWithAvailability, InsertBlob, InsertRepositoryName, RepoID,
};
use crate::flightdeck::tracer::Tracer;
use crate::repository::local::LocalRepository;
use crate::repository::rclone::parquet::Parquet;
use crate::repository::traits::{
    Adder, Availability, LastIndices, LastIndicesSyncer, Local, Metadata, RcloneTargetPath,
    Receiver, RepositoryMetadata, Sender, Syncer,
};
use crate::utils::errors::InternalError;
use crate::utils::path::RepoPath;
use crate::utils::rclone::{
    ConfigSection, ERROR_CODE_DIRECTORY_NOT_FOUND, ERROR_CODE_FILE_NOT_FOUND, Operation,
    RCloneConfig, RCloneTarget, run_rclone,
};
use futures::{FutureExt, StreamExt, TryStreamExt, pin_mut, stream};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use rand::Rng;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

pub(crate) mod parquet;

const EXTERNAL_PATH: &str = ".amb";
const FILE_ID: &str = "id";
const FILE_BLOB_STORE: &str = "blobs.parquet";
const FILE_FILE_STORE: &str = "files.parquet";
const FILE_REPOSITORY_NAME_STORE: &str = "repository_names.parquet";
const FILE_REPOSITORY_STORE: &str = "repositories.parquet";

#[derive(Clone)]
pub struct RCloneStore {
    local: LocalRepository,
    repo_id: RepoID,
    name: String,
    remote_name: String,
    remote_path: String,
    remote_files: RepoPath,
}

impl LastIndicesSyncer for RCloneStore {
    fn lookup(&self, _repo_id: RepoID) -> BoxFuture<'_, Result<LastIndices, InternalError>> {
        async move {
            Ok(LastIndices {
                file: None,
                blob: None,
                name: None,
            })
        }
        .boxed()
    }

    fn refresh(&self) -> BoxFuture<'_, Result<(), InternalError>> {
        async move { Ok(()) }.boxed()
    }
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
            FileList::All,
        )
        .await?;

        let external_path = files.join(EXTERNAL_PATH);
        let id_path = external_path.join(FILE_ID);
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
                FileList::ID,
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

            println!("RCloneStore::connect: created repository ID: {repo_id:?}");

            repo_id
        } else {
            let repo_id = tokio::fs::read_to_string(&id_path)
                .await
                .inspect_err(|e| log::error!("RCloneStore::connect: read ID file failed: {e}"))?;
            println!("RCloneStore::connect: read repository ID: {repo_id:?}");
            RepoID(repo_id)
        };

        Ok(Self {
            local: local.clone(),
            name: name.into(),
            repo_id,
            remote_name: remote_name.into(),
            remote_path: remote_path.into(),
            remote_files: files,
        })
    }

    pub(crate) async fn close(&self) -> Result<(), InternalError> {
        self.local.close().await?;
        Ok(())
    }
}

async fn sync(
    staging: &RepoPath,
    files: &RepoPath,
    remote_name: &str,
    remote_path: &str,
    direction: Direction,
    file_list: FileList,
) -> Result<(), InternalError> {
    fs::create_dir_all(&staging)
        .await
        .inspect_err(|e| log::error!("RCloneStore::sync: create_dir_all failed: {e}"))?;
    let rclone_files_path = staging.join("rclone.files");
    {
        let content = as_file_list(file_list)
            .into_iter()
            .map(|f| format!("{EXTERNAL_PATH}/{f}"))
            .collect::<Vec<String>>()
            .join("\n");
        let mut writer = File::create(&rclone_files_path)
            .await
            .inspect_err(|e| log::error!("RCloneStore::sync: open rclone.files failed: {e}"))?;
        writer.write_all(content.as_bytes()).await?;
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
    let res = run_rclone(
        Operation::Copy,
        staging.abs(),
        rclone_files_path.abs(),
        source,
        destination,
        config,
        |x| {
            println!("RCloneStore::sync: rclone event: {x:?}");
        },
    )
    .await;
    tracer_copy.measure();

    if let Err(e) = res {
        #[allow(clippy::wildcard_enum_match_arm)]
        match e {
            InternalError::RClone(code)
                if code == ERROR_CODE_DIRECTORY_NOT_FOUND || code == ERROR_CODE_FILE_NOT_FOUND =>
            {
                log::debug!(
                    "RCloneStore::sync: ignoring not yet initialised rclone repository: {e}"
                );
            }
            _ => {
                log::error!("RCloneStore::sync: rclone failed: {e}");
                return Err(e);
            }
        }
    }

    Ok(())
}

enum FileList {
    All,
    ID,
    BlobStore,
    FileStore,
    RepositoryNameStore,
    RepositoryStore,
}

fn as_file_list(f: FileList) -> Vec<&'static str> {
    match f {
        FileList::All => vec![
            FILE_ID,
            FILE_BLOB_STORE,
            FILE_FILE_STORE,
            FILE_REPOSITORY_STORE,
            FILE_REPOSITORY_STORE,
        ],
        FileList::ID => vec![FILE_ID],
        FileList::BlobStore => vec![FILE_BLOB_STORE],
        FileList::FileStore => vec![FILE_FILE_STORE],
        FileList::RepositoryNameStore => vec![FILE_REPOSITORY_NAME_STORE],
        FileList::RepositoryStore => vec![FILE_REPOSITORY_STORE],
    }
}

enum Direction {
    Download,
    Upload,
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

impl Syncer<models::Blob> for RCloneStore {
    fn select(
        &self,
        last_index: Option<u64>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<models::Blob, InternalError>>> {
        let path = self.remote_files.join(EXTERNAL_PATH).join(FILE_BLOB_STORE);
        async move {
            let parquet = Parquet::new(path);
            parquet.select(last_index).await
        }
        .boxed()
    }

    fn merge(
        &self,
        s: BoxStream<'static, models::Blob>,
    ) -> BoxFuture<'_, Result<(), InternalError>> {
        let transfer_id: u32 = rand::rng().random();
        let staging = self.local.staging_id_path(transfer_id);
        let remote_name = self.remote_name.clone();
        let remote_path = self.remote_path.clone();

        async move {
            let remote_files = staging.join("files");
            let external = remote_files.join(EXTERNAL_PATH);
            let path = external.join(FILE_BLOB_STORE);

            fs::create_dir_all(&external)
                .await
                .inspect_err(|e| log::error!("RCloneStore::merge: create_dir_all failed: {e}"))?;

            {
                let parquet = Parquet::new(path);
                parquet.merge(s).await?;
            }

            sync(
                &staging,
                &remote_files,
                &remote_name,
                &remote_path,
                Direction::Upload,
                FileList::BlobStore,
            )
            .await?;

            Ok(())
        }
        .boxed()
    }
}

impl Syncer<models::File> for RCloneStore {
    fn select(
        &self,
        last_index: Option<u64>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<models::File, InternalError>>> {
        let path = self.remote_files.join(EXTERNAL_PATH).join(FILE_FILE_STORE);
        async move {
            let parquet = Parquet::new(path);
            parquet.select(last_index).await
        }
        .boxed()
    }

    fn merge(
        &self,
        s: BoxStream<'static, models::File>,
    ) -> BoxFuture<'_, Result<(), InternalError>> {
        let transfer_id: u32 = rand::rng().random();
        let staging = self.local.staging_id_path(transfer_id);
        let remote_name = self.remote_name.clone();
        let remote_path = self.remote_path.clone();

        async move {
            let remote_files = staging.join("files");
            let external = remote_files.join(EXTERNAL_PATH);
            let path = external.join(FILE_FILE_STORE);

            fs::create_dir_all(&external)
                .await
                .inspect_err(|e| log::error!("RCloneStore::merge: create_dir_all failed: {e}"))?;

            {
                let parquet = Parquet::new(path);
                parquet.merge(s).await?;
            }

            sync(
                &staging,
                &remote_files,
                &remote_name,
                &remote_path,
                Direction::Upload,
                FileList::FileStore,
            )
            .await?;

            Ok(())
        }
        .boxed()
    }
}

impl Syncer<models::RepositoryName> for RCloneStore {
    fn select(
        &self,
        last_index: Option<u64>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<models::RepositoryName, InternalError>>> {
        let path = self
            .remote_files
            .join(EXTERNAL_PATH)
            .join(FILE_REPOSITORY_NAME_STORE);
        async move {
            let parquet = Parquet::new(path);
            parquet.select(last_index).await
        }
        .boxed()
    }

    fn merge(
        &self,
        s: BoxStream<'static, models::RepositoryName>,
    ) -> BoxFuture<'_, Result<(), InternalError>> {
        let transfer_id: u32 = rand::rng().random();
        let staging = self.local.staging_id_path(transfer_id);
        let remote_name = self.remote_name.clone();
        let remote_path = self.remote_path.clone();

        async move {
            let remote_files = staging.join("files");
            let external = remote_files.join(EXTERNAL_PATH);
            let path = external.join(FILE_REPOSITORY_NAME_STORE);

            fs::create_dir_all(&external)
                .await
                .inspect_err(|e| log::error!("RCloneStore::merge: create_dir_all failed: {e}"))?;

            {
                let parquet = Parquet::new(path);
                parquet.merge(s).await?;
            }

            sync(
                &staging,
                &remote_files,
                &remote_name,
                &remote_path,
                Direction::Upload,
                FileList::RepositoryNameStore,
            )
            .await?;

            Ok(())
        }
        .boxed()
    }
}

impl Syncer<models::Repository> for RCloneStore {
    fn select(
        &self,
        last_index: (),
    ) -> BoxFuture<'_, BoxStream<'static, Result<models::Repository, InternalError>>> {
        let path = self
            .remote_files
            .join(EXTERNAL_PATH)
            .join(FILE_REPOSITORY_STORE);
        async move {
            let parquet = Parquet::new(path);
            parquet.select(last_index).await
        }
        .boxed()
    }

    fn merge(
        &self,
        s: BoxStream<'static, models::Repository>,
    ) -> BoxFuture<'_, Result<(), InternalError>> {
        let transfer_id: u32 = rand::rng().random();
        let staging = self.local.staging_id_path(transfer_id);
        let remote_name = self.remote_name.clone();
        let remote_path = self.remote_path.clone();

        async move {
            let remote_files = staging.join("files");
            let external = remote_files.join(EXTERNAL_PATH);
            let path = external.join(FILE_REPOSITORY_STORE);

            fs::create_dir_all(&external)
                .await
                .inspect_err(|e| log::error!("RCloneStore::merge: create_dir_all failed: {e}"))?;

            {
                let parquet = Parquet::new(path);
                parquet.merge(s).await?;
            }

            sync(
                &staging,
                &remote_files,
                &remote_name,
                &remote_path,
                Direction::Upload,
                FileList::RepositoryStore,
            )
            .await?;

            Ok(())
        }
        .boxed()
    }
}
