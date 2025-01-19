use crate::db::database::Database;
use crate::db::establish_connection;
use crate::db::models::{Blob, BlobId, File, FilePathWithBlobId, Repository};
use crate::db::schema::run_migrations;
use crate::repository::traits::{
    Adder, Deprecated, LastIndices, LastIndicesSyncer, Local, Metadata, Reconciler, Syncer,
    SyncerParams,
};
use crate::utils::app_error::AppError;
use anyhow::Context;
use futures::{FutureExt, Stream, TryFutureExt, TryStreamExt};
use log::debug;
use std::future::Future;
use std::path::PathBuf;
use tokio::fs;

#[derive(Clone)]
pub(crate) struct LocalRepository {
    root: PathBuf,
    repo_id: String,
    db: Database,
}

/// Recursively searches parent directories for the `.inv` folder to determine the repository root.
async fn find_repository_root(start_path: &PathBuf) -> Result<PathBuf, anyhow::Error> {
    let mut current = start_path.canonicalize()?;

    loop {
        let inv_path = current.join(".inv");
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

    Err(anyhow::anyhow!("no `.inv` directory found in any parent directories"))
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

        let invariable_path = root.join(".inv");
        if !fs::metadata(&invariable_path)
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            return Err(anyhow::anyhow!("repository is not initialised").into());
        };

        let db_path = invariable_path.join("db.sqlite");
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
        let invariable_path = root.join(".inv");
        if fs::metadata(&invariable_path)
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            return Err(anyhow::anyhow!("repository is already initialised").into());
        };

        fs::create_dir(invariable_path.as_path()).await?;

        let blob_path = invariable_path.join("blobs");
        fs::create_dir_all(blob_path.as_path()).await?;

        let db_path = invariable_path.join("db.sqlite");
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

    fn invariable_path(&self) -> PathBuf {
        self.root().join(".inv")
    }

    fn blob_path(&self) -> PathBuf {
        self.invariable_path().join("blobs")
    }
}

impl Metadata for LocalRepository {
    async fn repo_id(&self) -> Result<String, AppError> {
        Ok(self.repo_id.clone())
    }
}

impl Adder for LocalRepository {
    async fn add_files<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = crate::db::models::InputFile> + Unpin,
    {
        self.db.add_files(s).await
    }

    async fn add_blobs<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = crate::db::models::InputBlob> + Unpin,
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
    ) -> impl Future<Output = impl Stream<Item = Result<Repository, AppError>> + Unpin + Send + 'static>
    {
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
    ) -> impl Future<Output = impl Stream<Item = Result<File, AppError>> + Unpin + Send + 'static>
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
    ) -> impl Future<Output = impl Stream<Item = Result<Blob, AppError>> + Unpin + Send + 'static>
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

impl Deprecated for LocalRepository {
    fn missing_blobs(
        &self,
        source_repo_id: String,
        target_repo_id: String,
    ) -> impl Stream<Item = Result<BlobId, AppError>> + Unpin + Send {
        self.db
            .missing_blobs(source_repo_id, target_repo_id)
            .err_into()
    }
}
