use crate::connection::EstablishedConnection;
use crate::db::database::{DBOutputStream, Database};
use crate::db::migrations::run_migrations;
use crate::db::models::{
    AvailableBlob, Blob, BlobAssociatedToFiles, BlobTransferItem, Connection, CopiedTransferItem,
    File, FileTransferItem, MissingFile, Observation, ObservedBlob, Repository, RepositoryName,
    VirtualFile,
};
use crate::db::{establish_connection, models};
use crate::logic::assimilate;
use crate::logic::assimilate::Item;
use crate::logic::files;
use crate::repository::traits::{
    Adder, Availability, BufferType, Config, ConnectionManager, LastIndices, LastIndicesSyncer,
    Local, Metadata, RcloneTargetPath, Receiver, RepositoryMetadata, Sender, Syncer, SyncerParams,
    TransferItem, VirtualFilesystem,
};
use crate::utils;
use crate::utils::errors::{AppError, InternalError};
use crate::utils::flow::{ExtFlow, Flow};
use crate::utils::path::RepoPath;
use crate::utils::pipe::TryForwardIntoExt;
use fs2::FileExt;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt, pin_mut, stream};
use log::debug;
use rand::Rng;
use rand::distr::Alphanumeric;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::{fs, task};

const REPO_FOLDER_NAME: &str = ".amb";

#[derive(Clone)]
pub(crate) struct LocalRepository {
    root: PathBuf,
    repo_id: String,
    db: Database,
    _lock: Arc<std::fs::File>,
}

/// Recursively searches parent directories for the folder to determine the repository root.
async fn find_repository_root(start_path: &Path) -> Result<PathBuf, InternalError> {
    let mut current = start_path.canonicalize()?;

    loop {
        let inv_path = current.join(REPO_FOLDER_NAME);
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
    task::spawn_blocking(move || {
        let file = std::fs::File::create(&path)?;
        match file.try_lock_exclusive() {
            Ok(_) => Ok(file),
            Err(_) => Err(InternalError::SharedAccess),
        }
    })
    .await?
}

impl LocalRepository {
    pub async fn new(maybe_root: Option<PathBuf>) -> Result<Self, InternalError> {
        let root = if let Some(root) = maybe_root {
            root
        } else {
            find_repository_root(&std::env::current_dir()?).await?
        };

        let repository_path = root.join(REPO_FOLDER_NAME);
        if !fs::metadata(&repository_path)
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            return Err(AppError::RepositoryNotInitialised().into());
        };

        let lock_path = repository_path.join(".lock");
        let lock = acquire_exclusive_lock(lock_path).await?;

        let staging_path = repository_path.join("staging");
        files::cleanup_staging(&staging_path).await?;
        debug!("deleted staging {}", staging_path.display());

        let db_path = repository_path.join("db.sqlite");
        let pool = establish_connection(db_path.to_str().unwrap())
            .await
            .expect("failed to establish connection");
        run_migrations(&pool)
            .await
            .expect("failed to run migrations");

        let db = Database::new(pool.clone());
        let repo = db
            .get_or_create_current_repository()
            .await
            .expect("failed to create repo id");

        db.clean().await?;

        debug!("db connected");

        Ok(Self {
            root: root.canonicalize()?,
            repo_id: repo.repo_id,
            db,
            _lock: Arc::new(lock),
        })
    }

    pub async fn create(maybe_root: Option<PathBuf>, name: String) -> Result<Self, InternalError> {
        let root = if let Some(path) = maybe_root {
            path
        } else {
            fs::canonicalize(".").await?
        };
        let repository_path = root.join(REPO_FOLDER_NAME);
        if fs::metadata(&repository_path)
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            return Err(AppError::RepositoryAlreadyInitialised().into());
        };

        fs::create_dir(repository_path.as_path()).await?;

        utils::fs::capability_check(&repository_path).await?;

        let blobs_path = repository_path.join("blobs");
        fs::create_dir_all(blobs_path.as_path()).await?;

        let db_path = repository_path.join("db.sqlite");
        let pool = establish_connection(db_path.to_str().unwrap()).await?;
        run_migrations(&pool).await?;

        let db = Database::new(pool.clone());

        let repo = db
            .get_or_create_current_repository()
            .await
            .expect("failed to create repo id");

        db.add_repository_names(stream::iter([models::InsertRepositoryName {
            repo_id: repo.repo_id.clone(),
            name: name.clone(),
            valid_from: chrono::Utc::now(),
        }]))
        .await?;

        Self::new(Some(root)).await
    }

    pub(crate) fn db(&self) -> &Database {
        &self.db
    }
}

impl Local for LocalRepository {
    fn root(&self) -> RepoPath {
        RepoPath::from_root(self.root.clone())
    }

    fn repository_path(&self) -> RepoPath {
        self.root().join(REPO_FOLDER_NAME)
    }

    fn blobs_path(&self) -> RepoPath {
        self.repository_path().join("blobs")
    }

    fn blob_path(&self, blob_id: &str) -> RepoPath {
        if blob_id.len() > 6 {
            self.blobs_path()
                .join(&blob_id[0..2])
                .join(&blob_id[2..4])
                .join(&blob_id[4..])
        } else {
            self.blobs_path().join(blob_id)
        }
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
}

impl Config for LocalRepository {
    fn buffer_size(&self, buffer: BufferType) -> usize {
        match buffer {
            BufferType::AssimilateParallelism => 20,
            BufferType::TransferRcloneFilesStreamChunkSize => 10000,
            BufferType::TransferRcloneFilesWriterChunkSize => 1000,
            BufferType::AddFilesBlobifyFutureFileParallelism => 20,
            BufferType::AddFilesDBAddFilesChannelSize => 1000,
            BufferType::AddFilesDBAddBlobsChannelSize => 1000,
            BufferType::AddFilesDBAddMaterialisationsChannelSize => 1000,
            BufferType::PrepareTransferParallelism => 100,
            BufferType::StateBufferChannelSize => 10000,
            BufferType::StateCheckerParallelism => 100,
            BufferType::WalkerChannelSize => 10000,
            BufferType::MaterialiseParallelism => 100,
            BufferType::FsckBufferParallelism => 20,
            BufferType::FsckMaterialiseBufferParallelism => 100,
            BufferType::FsckRcloneFilesWriterBufferSize => 10000,
            BufferType::FsckRcloneFilesStreamChunkSize => 1000,
        }
    }
}

impl Metadata for LocalRepository {
    async fn current(&self) -> Result<RepositoryMetadata, InternalError> {
        let name = self
            .db
            .lookup_current_repository_name(self.repo_id.clone())
            .await?;
        Ok(RepositoryMetadata {
            id: self.repo_id.clone(),
            name: name.unwrap_or("-".into()),
        })
    }
}

impl Availability for LocalRepository {
    fn available(
        &self,
    ) -> impl Stream<Item = Result<AvailableBlob, InternalError>> + Unpin + Send + 'static {
        self.db.available_blobs(self.repo_id.clone()).err_into()
    }

    fn missing(
        &self,
    ) -> impl Stream<Item = Result<BlobAssociatedToFiles, InternalError>> + Unpin + Send + 'static
    {
        self.db.missing_blobs(self.repo_id.clone()).err_into()
    }
}

impl Adder for LocalRepository {
    async fn add_files<S>(&self, s: S) -> Result<u64, sqlx::Error>
    where
        S: Stream<Item = models::InsertFile> + Unpin + Send,
    {
        self.db.add_files(s).await
    }

    async fn add_blobs<S>(&self, s: S) -> Result<u64, sqlx::Error>
    where
        S: Stream<Item = models::InsertBlob> + Unpin + Send,
    {
        self.db.add_blobs(s).await
    }

    async fn observe_blobs<S>(&self, s: S) -> Result<u64, sqlx::Error>
    where
        S: Stream<Item = ObservedBlob> + Unpin + Send,
    {
        self.db.observe_blobs(s).await
    }

    async fn add_repository_names<S>(&self, s: S) -> Result<u64, sqlx::Error>
    where
        S: Stream<Item = models::InsertRepositoryName> + Unpin + Send,
    {
        self.db.add_repository_names(s).await
    }

    async fn add_materialisation<S>(&self, s: S) -> Result<u64, sqlx::Error>
    where
        S: Stream<Item = models::InsertMaterialisation> + Unpin + Send,
    {
        self.db.add_materialisations(s).await
    }

    async fn lookup_last_materialisation(
        &self,
        path: &RepoPath,
    ) -> Result<Option<String>, InternalError> {
        let result = self
            .db
            .lookup_last_materialisation(path.rel().to_string_lossy().to_string())
            .await
            .map_err(InternalError::from)?;

        Ok(result.and_then(|b| b.blob_id))
    }
}

impl LastIndicesSyncer for LocalRepository {
    async fn lookup(&self, repo_id: String) -> Result<LastIndices, InternalError> {
        let Repository {
            last_file_index,
            last_blob_index,
            last_name_index,
            ..
        } = self.db.lookup_repository(repo_id).await?;
        Ok(LastIndices {
            file: last_file_index,
            blob: last_blob_index,
            name: last_name_index,
        })
    }

    async fn refresh(&self) -> Result<(), InternalError> {
        self.db.update_last_indices().await?;
        Ok(())
    }
}

impl SyncerParams for Repository {
    type Params = ();
}

impl Syncer<Repository> for LocalRepository {
    fn select(
        &self,
        _params: (),
    ) -> impl Future<
        Output = impl Stream<Item = Result<Repository, InternalError>> + Unpin + Send + 'static,
    > + Send {
        self.db.select_repositories().map(|s| s.err_into())
    }

    fn merge<S>(&self, s: S) -> impl Future<Output = Result<(), InternalError>> + Send
    where
        S: Stream<Item = Repository> + Unpin + Send + 'static,
    {
        self.db.merge_repositories(s).err_into()
    }
}

impl SyncerParams for File {
    type Params = i32;
}

impl Syncer<File> for LocalRepository {
    fn select(
        &self,
        last_index: i32,
    ) -> impl Future<
        Output = impl Stream<Item = Result<File, InternalError>> + Unpin + Send + 'static,
    > + Send {
        self.db.select_files(last_index).map(|s| s.err_into())
    }

    fn merge<S>(&self, s: S) -> impl Future<Output = Result<(), InternalError>> + Send
    where
        S: Stream<Item = File> + Unpin + Send + 'static,
    {
        self.db.merge_files(s).err_into()
    }
}

impl SyncerParams for Blob {
    type Params = i32;
}

impl Syncer<Blob> for LocalRepository {
    fn select(
        &self,
        last_index: i32,
    ) -> impl Future<
        Output = impl Stream<Item = Result<Blob, InternalError>> + Unpin + Send + 'static,
    > + Send {
        self.db.select_blobs(last_index).map(|s| s.err_into())
    }

    fn merge<S>(&self, s: S) -> impl Future<Output = Result<(), InternalError>> + Send
    where
        S: Stream<Item = Blob> + Unpin + Send + 'static,
    {
        self.db.merge_blobs(s).err_into()
    }
}
impl SyncerParams for RepositoryName {
    type Params = i32;
}

impl Syncer<RepositoryName> for LocalRepository {
    fn select(
        &self,
        last_index: i32,
    ) -> impl Future<
        Output = impl Stream<Item = Result<RepositoryName, InternalError>> + Unpin + Send + 'static,
    > + Send {
        self.db
            .select_repository_names(last_index)
            .map(|s| s.err_into())
    }

    fn merge<S>(&self, s: S) -> impl Future<Output = Result<(), InternalError>> + Send
    where
        S: Stream<Item = RepositoryName> + Unpin + Send + 'static,
    {
        self.db.merge_repository_names(s).err_into()
    }
}

impl VirtualFilesystem for LocalRepository {
    async fn reset(&self) -> Result<(), sqlx::Error> {
        self.db.truncate_virtual_filesystem().await
    }

    async fn refresh(&self) -> Result<(), sqlx::Error> {
        self.db.refresh_virtual_filesystem().await
    }

    fn cleanup(&self, last_seen_id: i64) -> impl Future<Output = Result<(), sqlx::Error>> + Send {
        self.db.cleanup_virtual_filesystem(last_seen_id)
    }

    fn select_missing_files(
        &self,
        last_seen_id: i64,
    ) -> impl Future<Output = DBOutputStream<'static, MissingFile>> + Send {
        self.db
            .select_missing_files_on_virtual_filesystem(last_seen_id)
    }

    async fn add_observations(
        &self,
        input_stream: impl Stream<Item = Flow<Observation>> + Unpin + Send + 'static,
    ) -> impl Stream<Item = ExtFlow<Result<Vec<VirtualFile>, sqlx::Error>>> + Unpin + Send + 'static
    {
        self.db
            .add_virtual_filesystem_observations(input_stream)
            .await
    }
}

impl ConnectionManager for LocalRepository {
    async fn add(&self, connection: &Connection) -> Result<(), InternalError> {
        self.db.add_connection(connection).await.map_err(Into::into)
    }

    async fn lookup_by_name(&self, name: &str) -> Result<Option<Connection>, InternalError> {
        self.db.connection_by_name(name).await.map_err(Into::into)
    }

    async fn list(&self) -> Result<Vec<Connection>, InternalError> {
        self.db.list_all_connections().await.map_err(Into::into)
    }

    async fn connect(&self, name: String) -> Result<EstablishedConnection, InternalError> {
        if let Ok(Some(Connection {
            connection_type,
            parameter,
            ..
        })) = self.lookup_by_name(&name).await
        {
            EstablishedConnection::new(self.clone(), name, connection_type, parameter).await
        } else {
            Err(AppError::ConnectionNotFound(name).into())
        }
    }
}

impl TransferItem for BlobTransferItem {
    fn path(&self) -> String {
        self.path.clone()
    }
}

impl RcloneTargetPath for LocalRepository {
    async fn rclone_path(&self, transfer_id: u32) -> Result<String, InternalError> {
        Ok(self
            .rclone_target_path(transfer_id)
            .abs()
            .to_string_lossy()
            .into_owned())
    }
}

impl Sender<BlobTransferItem> for LocalRepository {
    async fn prepare_transfer<S>(&self, s: S) -> Result<u64, InternalError>
    where
        S: Stream<Item = BlobTransferItem> + Unpin + Send + 'static,
    {
        let stream = tokio_stream::StreamExt::map(s, |item: BlobTransferItem| async move {
            let blob_path = self.blob_path(&item.blob_id);
            let transfer_path = self.rclone_target_path(item.transfer_id).join(item.path);
            if let Some(parent) = transfer_path.abs().parent() {
                fs::create_dir_all(parent).await?;
            }

            fs::hard_link(blob_path, transfer_path).await?;
            Result::<(), InternalError>::Ok(())
        });

        // allow multiple hard link operations to run concurrently
        let stream =
            stream.buffer_unordered(self.buffer_size(BufferType::PrepareTransferParallelism));

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
}

impl Receiver<BlobTransferItem> for LocalRepository {
    async fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: String,
        paths: Vec<String>,
    ) -> impl Stream<Item = Result<BlobTransferItem, InternalError>> + Unpin + Send + 'static {
        let rclone_target_path = self.rclone_target_path(transfer_id);
        if let Err(e) = fs::create_dir_all(rclone_target_path.abs()).await {
            return stream::iter([Err(e.into())]).boxed();
        }

        self.db
            .populate_missing_blobs_for_transfer(transfer_id, repo_id, paths)
            .await
            .err_into()
            .boxed()
    }

    async fn finalise_transfer(
        &self,
        s: impl Stream<Item = CopiedTransferItem> + Unpin + Send + 'static,
    ) -> Result<u64, InternalError> {
        let local = self.clone();
        let assimilation = self.clone();
        let db = self.db.clone();
        db.select_blobs_transfer(s)
            .await
            .map_ok(move |r| Item {
                path: local.rclone_target_path(r.transfer_id).join(r.path),
                expected_blob_id: Some(r.blob_id),
            })
            .try_forward_into::<_, _, _, _, InternalError>(|s| async {
                assimilate::assimilate(&assimilation, s).await
            })
            .await
    }
}

impl TransferItem for FileTransferItem {
    fn path(&self) -> String {
        self.path.clone()
    }
}

impl Sender<FileTransferItem> for LocalRepository {
    async fn prepare_transfer<S>(&self, s: S) -> Result<u64, InternalError>
    where
        S: Stream<Item = FileTransferItem> + Unpin + Send + 'static,
    {
        let stream = tokio_stream::StreamExt::map(s, |item: FileTransferItem| async move {
            let blob_path = self.blob_path(&item.blob_id);
            let transfer_path = self.rclone_target_path(item.transfer_id).join(item.path);
            if let Some(parent) = transfer_path.abs().parent() {
                fs::create_dir_all(parent).await?;
            }

            fs::hard_link(blob_path, transfer_path).await?;
            Result::<(), InternalError>::Ok(())
        });

        // allow multiple hard link operations to run concurrently
        let stream =
            stream.buffer_unordered(self.buffer_size(BufferType::PrepareTransferParallelism));

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
}

impl Receiver<FileTransferItem> for LocalRepository {
    async fn create_transfer_request(
        &self,
        transfer_id: u32,
        repo_id: String,
        paths: Vec<String>,
    ) -> impl Stream<Item = Result<FileTransferItem, InternalError>> + Unpin + Send + 'static {
        let rclone_target_path = self.rclone_target_path(transfer_id);
        if let Err(e) = fs::create_dir_all(rclone_target_path.abs()).await {
            return stream::iter([Err(e.into())]).boxed();
        }

        self.db
            .populate_missing_files_for_transfer(transfer_id, self.repo_id.clone(), repo_id, paths)
            .await
            .err_into()
            .boxed()
    }

    async fn finalise_transfer(
        &self,
        s: impl Stream<Item = CopiedTransferItem> + Unpin + Send + 'static,
    ) -> Result<u64, InternalError> {
        let local = self.clone();
        let assimilation = self.clone();
        let db = self.db.clone();
        db.select_files_transfer(s)
            .await
            .map_ok(move |r| Item {
                path: local.rclone_target_path(r.transfer_id).join(r.path),
                expected_blob_id: Some(r.blob_id),
            })
            .try_forward_into::<_, _, _, _, InternalError>(|s| async {
                assimilate::assimilate(&assimilation, s).await
            })
            .await
    }
}
