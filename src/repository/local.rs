use crate::connection::EstablishedConnection;
use crate::db;
use crate::db::database::Database;
use crate::db::error::DBError;
use crate::db::models;
use crate::db::models::{
    AvailableBlob, Blob, BlobAssociatedToFiles, BlobID, BlobTransferItem, Connection,
    ConnectionName, CopiedTransferItem, CurrentFile, File, FileCheck, FileSeen, FileTransferItem,
    FilesWithAvailability, MissingFile, RepoID, RepositoryName, RepositorySyncState, SizedBlobID,
    VirtualFile,
};
use crate::logic::assimilate;
use crate::logic::assimilate::Item;
use crate::logic::files;
use crate::repository::traits::{
    Adder, Availability, BufferType, Config, ConnectionManager, LastSyncState, LastSyncStateSyncer,
    Local, Metadata, RcloneTargetPath, Receiver, RepositoryMetadata, Sender, Syncer, SyncerParams,
    TransferItem, VirtualFilesystem,
};
use crate::utils::errors::{AppError, InternalError};
use crate::utils::fs::{Capability, capability_check, link};
use crate::utils::path::RepoPath;
use fs2::FileExt;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt, pin_mut, stream};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use log::debug;
use rand::Rng;
use rand::distr::Alphanumeric;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tokio::{fs, task};

#[derive(Clone)]
pub(crate) struct LocalRepository {
    root: PathBuf,
    app_folder: PathBuf,
    repo_id: RepoID,
    db: Database,
    capability: Capability,
    _lock: Arc<std::fs::File>,
}

/// Recursively searches parent directories for the folder to determine the repository root.
async fn find_repository_root(
    start_path: &Path,
    app_folder: &Path,
) -> Result<PathBuf, InternalError> {
    let mut current = start_path.canonicalize()?;

    loop {
        let inv_path = current.join(app_folder);
        if fs::metadata(&inv_path)
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            return Ok(current);
        }

        if let Some(parent) = current.parent() {
            current = parent.to_path_buf();
        } else {
            break;
        }
    }

    Err(AppError::RepositoryNotInitialised().into())
}

async fn acquire_exclusive_lock<P: AsRef<Path>>(path: P) -> Result<std::fs::File, InternalError> {
    let path = path.as_ref().to_owned();
    let file = task::spawn_blocking(move || std::fs::File::create(&path)).await??;

    let deadline = Instant::now() + Duration::from_secs(1);
    let mut delay = Duration::from_millis(5);
    let max_delay = Duration::from_millis(200);

    loop {
        match file.try_lock_exclusive() {
            Ok(()) => return Ok(file),
            Err(_) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(InternalError::SharedAccess);
                }

                let remaining = deadline.saturating_duration_since(now);
                sleep(delay.min(remaining)).await;
                delay = (delay * 2).min(max_delay);
            }
        }
    }
}

pub struct LocalRepositoryConfig {
    pub maybe_root: Option<PathBuf>,
    pub app_folder: PathBuf,
    pub preferred_capability: Option<Capability>,
}

impl LocalRepository {
    pub async fn new(config: LocalRepositoryConfig) -> Result<Self, InternalError> {
        let LocalRepositoryConfig {
            maybe_root,
            app_folder,
            preferred_capability,
        } = config;

        let root = if let Some(root) = maybe_root {
            root
        } else {
            find_repository_root(&std::env::current_dir()?, &app_folder).await?
        };

        let repository_path = root.join(&app_folder);
        if !fs::metadata(&repository_path)
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            return Err(AppError::RepositoryNotInitialised().into());
        };

        let lock_path = repository_path.join(".lock");
        let lock = acquire_exclusive_lock(lock_path).await?;

        let capability = capability_check(&repository_path, preferred_capability)
            .await
            .inspect_err(|e| log::error!("capability check failed: {e}"))?;
        let capability = match capability {
            Some(capability) => capability,
            None => {
                return Err(AppError::RefAndHardlinksNotSupported.into());
            }
        };
        debug!("detected capability {:?}", capability);

        let staging_path = repository_path.join("staging");
        files::cleanup_staging(&staging_path).await?;
        debug!("deleted staging {}", staging_path.display());

        let db = db::open(&repository_path).await?;

        let repo = db
            .get_or_create_current_repository()
            .await
            .expect("failed to create repo id");

        debug!("db connected");

        Ok(Self {
            root: root.canonicalize()?,
            repo_id: repo.repo_id,
            app_folder,
            db,
            capability,
            _lock: Arc::new(lock),
        })
    }

    pub async fn create(
        config: LocalRepositoryConfig,
        name: String,
    ) -> Result<Self, InternalError> {
        let LocalRepositoryConfig {
            maybe_root,
            app_folder,
            preferred_capability,
        } = config;

        let root = if let Some(path) = maybe_root {
            path
        } else {
            fs::canonicalize(".")
                .await
                .inspect_err(|e| log::error!("failed to canonicalize root: {e}"))?
        };
        let repository_path = root.join(&app_folder);
        if fs::metadata(&repository_path)
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            return Err(AppError::RepositoryAlreadyInitialised().into());
        };

        fs::create_dir(repository_path.as_path())
            .await
            .inspect_err(|e| log::error!("unable to create path {repository_path:?}: {e}"))?;

        let blobs_path = repository_path.join("blobs");
        fs::create_dir_all(blobs_path.as_path())
            .await
            .inspect_err(|e| log::error!("failed to create blobs path {blobs_path:?}: {e}"))?;

        let db = db::open(&repository_path).await?;

        let repo = db
            .get_or_create_current_repository()
            .await
            .expect("failed to create repo id");

        db.add_repository_names(
            stream::iter([models::InsertRepositoryName {
                repo_id: repo.repo_id.clone(),
                name: name.clone(),
                valid_from: chrono::Utc::now(),
            }])
            .boxed(),
        )
        .await?;

        db.close().await?;
        drop(db);

        Self::new(LocalRepositoryConfig {
            maybe_root: Some(root),
            app_folder,
            preferred_capability,
        })
        .await
    }

    pub(crate) async fn close(&self) -> Result<(), InternalError> {
        self.db.close().await?;
        Ok(())
    }
    pub(crate) async fn compact(&self) -> Result<(), InternalError> {
        self.db.compact().await?;
        Ok(())
    }
    pub(crate) fn db(&self) -> &Database {
        &self.db
    }

    pub(crate) fn app_folder(&self) -> &Path {
        self.app_folder.as_path()
    }
}

impl Local for LocalRepository {
    fn root(&self) -> RepoPath {
        RepoPath::from_root(self.root.clone())
    }

    fn repository_path(&self) -> RepoPath {
        self.root().join(&self.app_folder)
    }

    fn blobs_path(&self) -> RepoPath {
        self.repository_path().join("blobs")
    }

    fn blob_path(&self, blob_id: &BlobID) -> RepoPath {
        self.blobs_path().join(blob_id.path())
    }

    fn staging_path(&self) -> RepoPath {
        self.repository_path().join("staging")
    }

    fn staging_id_path(&self, staging_id: u32) -> RepoPath {
        self.staging_path().join(format!("t{}", staging_id))
    }

    fn rclone_target_path(&self, staging_id: u32) -> RepoPath {
        self.staging_id_path(staging_id).join("files")
    }

    fn log_path(&self) -> RepoPath {
        let ts = chrono::Utc::now()
            .format("%Y-%m-%d_%H_%M_%S_%6f")
            .to_string();

        let random: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(4)
            .map(char::from)
            .collect();

        self.repository_path()
            .join("logs")
            .join(format!("run_{ts}-{random}.txt.gz"))
    }

    fn capability(&self) -> &Capability {
        &self.capability
    }
}

impl Config for LocalRepository {
    fn buffer_size(&self, buffer: BufferType) -> usize {
        match buffer {
            BufferType::AssimilateParallelism => 20,
            BufferType::TransferRcloneFilesStreamChunkSize => 10000,
            BufferType::TransferRcloneFilesWriterChunkSize => 1000,
            BufferType::AddFilesBlobifyFutureFileParallelism => 20,
            BufferType::AddFilesDBAddFilesChannelSize => 10000,
            BufferType::AddFilesDBAddMaterialisationsChannelSize => 10000,
            BufferType::PrepareTransferParallelism => 20,
            BufferType::StateBufferChannelSize => 10000,
            BufferType::StateCheckerParallelism => 20,
            BufferType::WalkerChannelSize => 10000,
            BufferType::MaterialiseParallelism => 100,
            BufferType::FsckBufferParallelism => 20,
            BufferType::FsckMaterialiseBufferParallelism => 20,
            BufferType::FsckRcloneFilesWriterChannelSize => 10000,
            BufferType::FsckRcloneFilesStreamChunkSize => 1000,
            BufferType::FsMvParallelism => 10,
            BufferType::FsRmParallelism => 10,
        }
    }
}

impl Metadata for LocalRepository {
    fn current(&self) -> BoxFuture<'_, Result<RepositoryMetadata, InternalError>> {
        let db = self.db.clone();
        let repo_id = self.repo_id.clone();
        Box::pin(async move {
            let name = db.lookup_current_repository_name(repo_id.clone()).await?;
            Ok(RepositoryMetadata {
                id: repo_id.clone(),
                name: name.unwrap_or("-".into()),
            })
        })
    }
}

impl Availability for LocalRepository {
    fn available(&self) -> BoxStream<'static, Result<AvailableBlob, InternalError>> {
        self.db
            .available_blobs(self.repo_id.clone())
            .err_into()
            .boxed()
    }

    fn missing(
        &self,
    ) -> BoxFuture<'_, BoxStream<'static, Result<BlobAssociatedToFiles, InternalError>>> {
        let db = self.db.clone();
        async move {
            db.missing_blobs(self.repo_id.clone())
                .await
                .err_into()
                .boxed()
        }
        .boxed()
    }

    fn current_files_with_availability(
        &self,
    ) -> BoxFuture<'_, BoxStream<'static, Result<FilesWithAvailability, InternalError>>> {
        let db = self.db.clone();
        async move {
            db.current_files_with_availability(self.repo_id.clone())
                .await
                .err_into()
                .boxed()
        }
        .boxed()
    }
}

impl Adder for LocalRepository {
    fn add_files<'a>(
        &'a self,
        s: BoxStream<'a, models::InsertFile>,
    ) -> BoxFuture<'a, Result<u64, DBError>> {
        let db = self.db.clone();
        async move { db.add_files(s).await }.boxed()
    }

    fn add_blobs<'a>(
        &'a self,
        s: BoxStream<'a, models::InsertBlob>,
    ) -> BoxFuture<'a, Result<u64, DBError>> {
        let db = self.db.clone();
        async move { db.add_blobs(s).await }.boxed()
    }

    fn add_file_bundles<'a>(
        &'a self,
        s: BoxStream<'a, models::InsertFileBundle>,
    ) -> BoxFuture<'a, Result<u64, DBError>> {
        let db = self.db.clone();
        async move { db.add_file_bundles(s).await }.boxed()
    }

    fn add_repository_names<'a>(
        &'a self,
        s: BoxStream<'a, models::InsertRepositoryName>,
    ) -> BoxFuture<'a, Result<u64, DBError>> {
        let db = self.db.clone();
        async move { db.add_repository_names(s).await }.boxed()
    }

    fn add_materialisation<'a>(
        &'a self,
        s: BoxStream<'a, models::InsertMaterialisation>,
    ) -> BoxFuture<'a, Result<u64, DBError>> {
        let db = self.db.clone();
        async move { db.add_materialisations(s).await }.boxed()
    }
}

impl LastSyncStateSyncer for LocalRepository {
    fn lookup(&self, repo_id: RepoID) -> BoxFuture<'_, Result<LastSyncState, InternalError>> {
        let db = self.db.clone();
        async move {
            let RepositorySyncState {
                last_file_index,
                last_blob_index,
                last_name_index,
                ..
            } = db.lookup_repository_sync_state(repo_id).await?;
            Ok(LastSyncState {
                file: last_file_index,
                blob: last_blob_index,
                name: last_name_index,
            })
        }
        .boxed()
    }

    fn refresh(&self) -> BoxFuture<'_, Result<(), InternalError>> {
        let db = self.db.clone();
        async move {
            db.update_sync_state().await?;
            Ok(())
        }
        .boxed()
    }
}

impl SyncerParams for RepositorySyncState {
    type Params = ();
}

impl Syncer<RepositorySyncState> for LocalRepository {
    fn select(
        &self,
        _params: (),
    ) -> BoxFuture<'_, BoxStream<'static, Result<RepositorySyncState, InternalError>>> {
        let db = self.db.clone();
        async move { db.select_repository_sync_states().await.err_into().boxed() }.boxed()
    }

    fn merge(
        &self,
        s: BoxStream<'static, RepositorySyncState>,
    ) -> BoxFuture<'_, Result<(), InternalError>> {
        let db = self.db.clone();
        async move { db.merge_repository_sync_states(s).await.map_err(Into::into) }.boxed()
    }
}

impl SyncerParams for File {
    type Params = Option<u64>;
}

impl Syncer<File> for LocalRepository {
    fn select(
        &self,
        last_index: Option<u64>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<File, InternalError>>> {
        let db = self.db.clone();
        async move {
            db.select_files(last_index)
                .map(|s| s.err_into().boxed())
                .await
        }
        .boxed()
    }

    fn merge(&self, s: BoxStream<'static, File>) -> BoxFuture<'_, Result<(), InternalError>> {
        self.db.merge_files(s).err_into().boxed()
    }
}

impl SyncerParams for Blob {
    type Params = Option<u64>;
}

impl Syncer<Blob> for LocalRepository {
    fn select(
        &self,
        last_index: Option<u64>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<Blob, InternalError>>> {
        let db = self.db.clone();
        async move { db.select_blobs(last_index).await.err_into().boxed() }.boxed()
    }

    fn merge(&self, s: BoxStream<'static, Blob>) -> BoxFuture<'_, Result<(), InternalError>> {
        self.db.merge_blobs(s).err_into().boxed()
    }
}

impl SyncerParams for RepositoryName {
    type Params = Option<u64>;
}

impl Syncer<RepositoryName> for LocalRepository {
    fn select(
        &self,
        last_index: Option<u64>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<RepositoryName, InternalError>>> {
        let db = self.db.clone();
        async move {
            db.select_repository_names(last_index)
                .await
                .err_into()
                .boxed()
        }
        .boxed()
    }

    fn merge(
        &self,
        s: BoxStream<'static, RepositoryName>,
    ) -> BoxFuture<'_, Result<(), InternalError>> {
        self.db.merge_repository_names(s).err_into().boxed()
    }
}

impl VirtualFilesystem for LocalRepository {
    fn select_missing_files(
        &self,
        last_seen_id: i64,
    ) -> BoxFuture<'_, BoxStream<'static, Result<MissingFile, DBError>>> {
        let db = self.db.clone();
        async move {
            db.select_missing_files_on_virtual_filesystem(last_seen_id)
                .await
        }
        .boxed()
    }

    fn add_checked_events(
        &self,
        s: BoxStream<'static, FileCheck>,
    ) -> BoxFuture<'_, Result<u64, DBError>> {
        let db = self.db.clone();
        Box::pin(async move { db.add_virtual_filesystem_file_checked_events(s).await })
    }

    fn add_seen_events(
        &self,
        s: BoxStream<'static, FileSeen>,
    ) -> BoxFuture<'_, Result<u64, DBError>> {
        let db = self.db.clone();
        Box::pin(async move { db.add_virtual_filesystem_file_seen_events(s).await })
    }

    fn select_virtual_filesystem(
        &self,
        s: BoxStream<'static, FileSeen>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<VirtualFile, DBError>>> {
        let db = self.db.clone();
        async move {
            let repo = match db.get_or_create_current_repository().await {
                Ok(id) => id,
                Err(err) => return stream::iter([Err(err)]).boxed(),
            };
            db.select_virtual_filesystem(s, repo.repo_id).boxed()
        }
        .boxed()
    }

    fn select_current_files(
        &self,
    ) -> BoxFuture<'_, BoxStream<'static, Result<(models::Path, BlobID), DBError>>> {
        let db = self.db.clone();
        async move { db.select_current_files().await }.boxed()
    }

    fn select_current_files_with_prefix(
        &self,
        file_or_dir: String,
    ) -> BoxFuture<'_, BoxStream<'static, Result<(models::Path, BlobID), DBError>>> {
        let db = self.db.clone();
        async move { db.select_current_files_with_prefix(file_or_dir).await }.boxed()
    }

    fn left_join_current_files<
        K: Clone + Send + Sync + 'static,
        E: From<DBError> + Debug + Send + Sync + 'static,
    >(
        &self,
        s: BoxStream<'static, Result<K, E>>,
        key_func: impl Fn(K) -> models::Path + Sync + Send + 'static,
    ) -> BoxStream<'static, Result<(K, Option<CurrentFile>), E>> {
        self.db.left_join_current_files(s, key_func)
    }
}

impl ConnectionManager for LocalRepository {
    fn add(&self, connection: Connection) -> BoxFuture<'_, Result<(), InternalError>> {
        let db = self.db.clone();
        async move { db.add_connection(connection).await.map_err(Into::into) }.boxed()
    }

    fn lookup_by_name(
        &self,
        name: ConnectionName,
    ) -> BoxFuture<'_, Result<Option<Connection>, InternalError>> {
        let db = self.db.clone();
        async move { db.connection_by_name(name).await.map_err(Into::into) }.boxed()
    }

    fn list(&self) -> BoxFuture<'_, Result<Vec<Connection>, InternalError>> {
        let db = self.db.clone();
        async move { db.list_all_connections().await.map_err(Into::into) }.boxed()
    }

    fn connect(
        &self,
        name: ConnectionName,
    ) -> BoxFuture<'_, Result<EstablishedConnection, InternalError>> {
        let local = self.clone();
        async move {
            if let Ok(Some(Connection {
                connection_type,
                parameter,
                ..
            })) = local.lookup_by_name(name.clone()).await
            {
                EstablishedConnection::new(local.clone(), name, connection_type, parameter).await
            } else {
                Err(AppError::ConnectionNotFound(name).into())
            }
        }
        .boxed()
    }
}

impl From<BlobTransferItem> for SizedBlobID {
    fn from(v: BlobTransferItem) -> Self {
        SizedBlobID {
            blob_id: v.blob_id.clone(),
            blob_size: v.blob_size,
        }
    }
}

impl TransferItem for BlobTransferItem {
    fn new(path: models::Path, transfer_id: u32, sized: SizedBlobID) -> Self {
        Self {
            transfer_id,
            blob_id: sized.blob_id,
            blob_size: sized.blob_size,
            path,
        }
    }

    fn path(&self) -> String {
        self.path.0.clone()
    }
}

impl RcloneTargetPath for LocalRepository {
    fn rclone_path(&self, transfer_id: u32) -> BoxFuture<'_, Result<String, InternalError>> {
        let local = self.clone();
        async move {
            let path = local.rclone_target_path(transfer_id);
            Ok(path.abs().to_string_lossy().into_owned())
        }
        .boxed()
    }
}

impl Sender<BlobTransferItem> for LocalRepository {
    fn prepare_transfer(
        &self,
        s: BoxStream<'static, BlobTransferItem>,
    ) -> BoxFuture<'_, Result<u64, InternalError>> {
        let local = self.clone();
        async move {
            let local_clone = local.clone();
            let stream = tokio_stream::StreamExt::map(s, |item: BlobTransferItem| {
                let local = local_clone.clone();
                async move {
                    let blob_path = local.blob_path(&item.blob_id);
                    let transfer_path =
                        local.rclone_target_path(item.transfer_id).join(item.path.0);
                    if let Some(parent) = transfer_path.abs().parent() {
                        fs::create_dir_all(parent).await?;
                    }

                    link(blob_path, transfer_path, local.capability()).await?;
                    Result::<(), InternalError>::Ok(())
                }
            });

            // allow multiple hard link operations to run concurrently
            let stream =
                stream.buffer_unordered(local.buffer_size(BufferType::PrepareTransferParallelism));

            let mut count = 0;
            pin_mut!(stream);
            while let Some(maybe_path) = tokio_stream::StreamExt::next(&mut stream).await {
                match maybe_path {
                    Ok(()) => count += 1,
                    Err(e) => {
                        return Err(e);
                    }
                }
            }

            Ok(count)
        }
        .boxed()
    }
}

impl Receiver<BlobTransferItem> for LocalRepository {
    fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: RepoID,
        paths: Vec<String>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<BlobTransferItem, InternalError>>> {
        async move {
            let rclone_target_path = self.rclone_target_path(transfer_id);
            if let Err(e) = fs::create_dir_all(rclone_target_path.abs()).await {
                return stream::iter([Err(e.into())]).boxed();
            }

            self.db
                .select_missing_blobs_for_transfer(transfer_id, repo_id, paths)
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
        let local = self.clone();
        let assimilation = self.clone();
        async move {
            let s = s
                .map(move |r| Item {
                    path: local.rclone_target_path(r.transfer_id).join(r.path.0),
                    expected_blob_id: Some(r.blob_id),
                })
                .boxed();

            assimilate::assimilate(&assimilation, s).await
        }
        .boxed()
    }
}

impl TransferItem for FileTransferItem {
    fn new(path: models::Path, transfer_id: u32, sized: SizedBlobID) -> Self {
        Self {
            transfer_id,
            blob_id: sized.blob_id,
            blob_size: sized.blob_size,
            path,
        }
    }

    fn path(&self) -> String {
        self.path.0.clone()
    }
}

impl From<FileTransferItem> for SizedBlobID {
    fn from(val: FileTransferItem) -> Self {
        SizedBlobID {
            blob_id: val.blob_id.clone(),
            blob_size: val.blob_size,
        }
    }
}

impl Sender<FileTransferItem> for LocalRepository {
    fn prepare_transfer(
        &self,
        s: BoxStream<'static, FileTransferItem>,
    ) -> BoxFuture<'_, Result<u64, InternalError>> {
        let local = self.clone();
        async move {
            let local_clone = local.clone();
            let stream = tokio_stream::StreamExt::map(s, |item: FileTransferItem| {
                let local = local_clone.clone();
                async move {
                    let blob_path = local.blob_path(&item.blob_id);
                    let transfer_path =
                        local.rclone_target_path(item.transfer_id).join(item.path.0);
                    if let Some(parent) = transfer_path.abs().parent() {
                        fs::create_dir_all(parent).await?;
                    }

                    link(blob_path, transfer_path, local.capability()).await?;
                    Result::<(), InternalError>::Ok(())
                }
            });

            // allow multiple hard link operations to run concurrently
            let stream =
                stream.buffer_unordered(local.buffer_size(BufferType::PrepareTransferParallelism));

            let mut count = 0;
            pin_mut!(stream);
            while let Some(maybe_path) = tokio_stream::StreamExt::next(&mut stream).await {
                match maybe_path {
                    Ok(()) => count += 1,
                    Err(e) => {
                        return Err(e);
                    }
                }
            }

            Ok(count)
        }
        .boxed()
    }
}

impl Receiver<FileTransferItem> for LocalRepository {
    fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: RepoID,
        paths: Vec<String>,
    ) -> BoxFuture<'_, BoxStream<'static, Result<FileTransferItem, InternalError>>> {
        let db = self.db.clone();
        let local_repo_id = self.repo_id.clone();
        let rclone_target_path = self.rclone_target_path(transfer_id);
        async move {
            if let Err(e) = fs::create_dir_all(rclone_target_path.abs()).await {
                return stream::iter([Err(e.into())]).boxed();
            }

            db.select_missing_files_for_transfer(transfer_id, local_repo_id.clone(), repo_id, paths)
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
        let local = self.clone();
        let assimilation = self.clone();
        async move {
            let s = s
                .map(move |r| Item {
                    path: local.rclone_target_path(r.transfer_id).join(r.path.0),
                    expected_blob_id: Some(r.blob_id),
                })
                .boxed();

            assimilate::assimilate(&assimilation, s).await
        }
        .boxed()
    }
}
