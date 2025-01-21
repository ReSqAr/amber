use crate::db::database::{DBOutputStream, Database};
use crate::db::establish_connection;
use crate::db::migrations::run_migrations;
use crate::db::models::{
    Blob, BlobId, BlobWithPaths, Connection, File, FilePathWithBlobId, Observation, Repository,
    VirtualFile,
};
use crate::repository::traits::{
    Adder, ConnectionManager, Deprecated, LastIndices, LastIndicesSyncer, Local, Metadata, Missing,
    Reconciler, Syncer, SyncerParams, VirtualFilesystem,
};
use crate::utils::app_error::AppError;
use crate::utils::flow::{ExtFlow, Flow};
use anyhow::{anyhow, Context};
use futures::{FutureExt, Stream, TryFutureExt, TryStreamExt};
use log::debug;
use std::future::Future;
use std::path::{Path, PathBuf};
use tokio::fs;

const REPO_FOLDER_NAME: &str = ".amb";

#[derive(Clone)]
pub(crate) struct LocalRepository {
    root: PathBuf,
    repo_id: String,
    db: Database,
}

/// Recursively searches parent directories for the folder to determine the repository root.
async fn find_repository_root(start_path: &Path) -> Result<PathBuf, anyhow::Error> {
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

    Err(anyhow::anyhow!(
        "no `.inv` directory found in any parent directories"
    ))
}

impl LocalRepository {
    pub async fn new(maybe_root: Option<PathBuf>) -> Result<Self, Box<dyn std::error::Error>> {
        let root = if let Some(root) = maybe_root {
            root
        } else {
            find_repository_root(&std::env::current_dir()?)
                .await
                .context("could not find an invariant folder")?
        };

        let repository_path = root.join(REPO_FOLDER_NAME);
        if !fs::metadata(&repository_path)
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            return Err(anyhow::anyhow!("repository is not initialised").into());
        };

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

        debug!("db connected");

        Ok(LocalRepository {
            root,
            repo_id: repo.repo_id,
            db,
        })
    }

    pub async fn create(maybe_root: Option<PathBuf>) -> Result<Self, Box<dyn std::error::Error>> {
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
            return Err(anyhow::anyhow!("repository is already initialised").into());
        };

        fs::create_dir(repository_path.as_path()).await?;

        let blobs_path = repository_path.join("blobs");
        fs::create_dir_all(blobs_path.as_path()).await?;

        let db_path = repository_path.join("db.sqlite");
        let pool = establish_connection(db_path.to_str().unwrap())
            .await
            .context("failed to establish connection")?;
        run_migrations(&pool)
            .await
            .context("failed to run migrations")?;

        let db = Database::new(pool.clone());

        db.setup_db().await?;

        let repo = db
            .get_or_create_current_repository()
            .await
            .expect("failed to create repo id");
        debug!(
            "Initialised repository id={} in {}",
            repo.repo_id,
            root.display()
        );

        Self::new(Some(root)).await
    }
}

impl Local for LocalRepository {
    fn root(&self) -> PathBuf {
        self.root.clone()
    }

    fn repository_path(&self) -> PathBuf {
        self.root().join(REPO_FOLDER_NAME)
    }

    fn blobs_path(&self) -> PathBuf {
        self.repository_path().join("blobs")
    }

    fn blob_path(&self, blob_id: String) -> PathBuf {
        self.blobs_path().join(blob_id)
    }

    fn staging_path(&self) -> PathBuf {
        self.repository_path().join("staging")
    }
}

impl Metadata for LocalRepository {
    async fn repo_id(&self) -> Result<String, AppError> {
        Ok(self.repo_id.clone())
    }
}

impl Missing for LocalRepository {
    fn missing(&self) -> impl Stream<Item = Result<BlobWithPaths, AppError>> + Unpin + Send {
        self.db.missing_blobs(self.repo_id.clone()).err_into()
    }
}

impl Adder for LocalRepository {
    async fn add_files<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = crate::db::models::InsertFile> + Unpin + Send + Sync,
    {
        self.db.add_files(s).await
    }

    async fn add_blobs<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = crate::db::models::InsertBlob> + Unpin + Send + Sync,
    {
        self.db.add_blobs(s).await
    }
}

impl LastIndicesSyncer for LocalRepository {
    async fn lookup(&self, repo_id: String) -> Result<LastIndices, AppError> {
        let Repository {
            last_file_index,
            last_blob_index,
            ..
        } = self.db.lookup_repository(repo_id).await?;
        Ok(LastIndices {
            file: last_file_index,
            blob: last_blob_index,
        })
    }

    async fn refresh(&self) -> Result<(), AppError> {
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
        Output = impl Stream<Item = Result<Repository, AppError>> + Unpin + Send + 'static,
    > + Send {
        self.db.select_repositories().map(|s| s.err_into())
    }

    fn merge<S>(&self, s: S) -> impl Future<Output = Result<(), AppError>> + Send
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
    ) -> impl Future<Output = impl Stream<Item = Result<File, AppError>> + Unpin + Send + 'static> + Send
    {
        self.db.select_files(last_index).map(|s| s.err_into())
    }

    fn merge<S>(&self, s: S) -> impl Future<Output = Result<(), AppError>> + Send
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
    ) -> impl Future<Output = impl Stream<Item = Result<Blob, AppError>> + Unpin + Send + 'static> + Send
    {
        self.db.select_blobs(last_index).map(|s| s.err_into())
    }

    fn merge<S>(&self, s: S) -> impl Future<Output = Result<(), AppError>> + Send
    where
        S: Stream<Item = Blob> + Unpin + Send + 'static,
    {
        self.db.merge_blobs(s).err_into()
    }
}

impl Reconciler for LocalRepository {
    fn target_filesystem_state(
        &self,
    ) -> impl Stream<Item = Result<FilePathWithBlobId, AppError>> + Unpin + Send {
        self.db
            .target_filesystem_state(self.repo_id.clone())
            .err_into()
    }
}

impl VirtualFilesystem for LocalRepository {
    async fn refresh(&self) -> Result<(), sqlx::Error> {
        self.db.refresh_virtual_filesystem().await
    }

    fn cleanup(&self, last_seen_id: i64) -> impl Future<Output = Result<(), sqlx::Error>> + Send {
        self.db.cleanup_virtual_filesystem(last_seen_id)
    }

    fn select_deleted_files(
        &self,
        last_seen_id: i64,
    ) -> impl Future<Output = DBOutputStream<'static, VirtualFile>> + Send {
        self.db
            .select_deleted_files_on_virtual_filesystem(last_seen_id)
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

impl Deprecated for LocalRepository {
    fn deprecated_missing_blobs(
        &self,
        source_repo_id: String,
        target_repo_id: String,
    ) -> impl Stream<Item = Result<BlobId, AppError>> + Unpin + Send {
        self.db
            .deprecated_missing_blobs(source_repo_id, target_repo_id)
            .err_into()
    }
}

impl ConnectionManager for LocalRepository {
    async fn add(&self, connection: &Connection) -> Result<(), AppError> {
        self.db.add_connection(connection).await.map_err(Into::into)
    }

    async fn lookup_by_name(&self, name: &String) -> Result<Option<Connection>, AppError> {
        self.db.connection_by_name(name).await.map_err(Into::into)
    }

    async fn list(&self) -> Result<Vec<Connection>, AppError> {
        self.db.list_all_connections().await.map_err(Into::into)
    }

    async fn connect(
        &self,
        name: String,
    ) -> Result<crate::repository::connection::Connection, Box<dyn std::error::Error>> {
        if let Ok(Some(crate::db::models::Connection {
            connection_type,
            parameter,
            ..
        })) = self.lookup_by_name(&name).await
        {
            crate::repository::connection::Connection::connect(name, connection_type, parameter)
                .await
        } else {
            Err(anyhow!("unable to find the connection '{}'", name).into())
        }
    }
}
