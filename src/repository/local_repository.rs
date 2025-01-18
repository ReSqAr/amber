use crate::db::db::{DBOutputStream, DB};
use crate::db::establish_connection;
use crate::db::schema::run_migrations;
use log::debug;
use std::path::PathBuf;
use futures::{Stream};
use sqlx::Error;
use tokio::fs;
use crate::db::models::Repository;

pub(crate) struct LocalRepository {
    root: PathBuf,
    repo_id: String,
    pub(crate) db: DB,                   // TODO: make private again
}


async fn resolve_maybe_path(
    maybe_path: Option<PathBuf>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(path) = maybe_path {
        Ok(path)
    } else {
        Ok(fs::canonicalize(".").await?)
    }
}

impl LocalRepository {
    pub async fn new(maybe_root: Option<PathBuf>) -> Result<Self, Box<dyn std::error::Error>> {
        let root = resolve_maybe_path(maybe_root).await?;
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

        let db = DB::new(pool.clone());
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
        let root = resolve_maybe_path(maybe_root).await?;
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
            .expect("failed to establish connection");
        run_migrations(&pool)
            .await
            .expect("failed to run migrations");

        let db = DB::new(pool.clone());

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

pub trait Local {
    fn root(&self) -> PathBuf;
    fn invariable_path(&self) -> PathBuf;
    fn blob_path(&self) -> PathBuf;
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

pub trait Metadata {
    fn repo_id(&self) -> String;
}

impl Metadata for LocalRepository {
    fn repo_id(&self) -> String {
        self.repo_id.clone()
    }
}


pub trait Adder {
    async fn add_files<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item =crate::db::models::InputFile> + Unpin;

    async fn add_blobs<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item =crate::db::models::InputBlob> + Unpin;
}

impl Adder for LocalRepository {
    async fn add_files<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item =crate::db::models::InputFile> + Unpin,
    {
        self.db.add_files(s).await
    }

    async fn add_blobs<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item =crate::db::models::InputBlob> + Unpin,
    {
        self.db.add_blobs(s).await
    }
}

pub trait Syncer<T> {
    fn select(&self) -> crate::db::db::DBOutputStream<'static, T>;

    async fn merge<S>(&self, s: S) -> Result<(), sqlx::Error>
    where
        S: Stream<Item = T> + Unpin;
}

impl Syncer<Repository> for LocalRepository {
    fn select(&self) -> DBOutputStream<'static, Repository> {
        self.db.select_repositories()
    }

    async fn merge<S>(&self, s: S) -> Result<(), Error>
    where
        S: Stream<Item=Repository> + Unpin,
    {
        self.db.merge_repositories(s).await
    }
}
