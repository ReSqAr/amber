use crate::db::db::DB;
use crate::db::establish_connection;
use crate::db::schema::run_migrations;
use log::debug;
use std::path::PathBuf;
use tokio::fs;

pub(crate) struct LocalRepository {
    pub(crate) root: PathBuf,// TODO: make getter [Local]
    pub(crate) repo_id: String,          // TODO: make getter [Metadata]
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
