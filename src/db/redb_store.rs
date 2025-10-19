use crate::db::error::DBError;
use once_cell::sync::Lazy;
use redb::Database as RedbDatabase;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

// TODO: but why global?
static REDB_DATABASES: Lazy<Mutex<HashMap<PathBuf, Weak<RedbDatabase>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
pub struct RedbStore {
    db: Arc<RedbDatabase>,
}

impl RedbStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, DBError> {
        let path = path.as_ref().to_path_buf();

        if let Some(existing) = {
            let mut guard = REDB_DATABASES.lock().expect("redb cache poisoned");
            if let Some(entry) = guard.get(&path) {
                if let Some(db) = entry.upgrade() {
                    Some(db)
                } else {
                    guard.remove(&path);
                    None
                }
            } else {
                None
            }
        } {
            return Ok(Self { db: existing });
        }

        let path_clone = path.clone();
        let db = tokio::task::spawn_blocking(move || {
            if path_clone.exists() {
                Ok::<_, DBError>(RedbDatabase::open(path_clone.as_path())?)
            } else {
                if let Some(parent) = path_clone.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                Ok(RedbDatabase::create(path_clone.as_path())?)
            }
        })
        .await??;

        let arc = Arc::new(db);
        let mut guard = REDB_DATABASES.lock().expect("redb cache poisoned");
        guard.insert(path, Arc::downgrade(&arc));

        Ok(Self { db: arc })
    }

    pub(crate) fn database(&self) -> Arc<RedbDatabase> {
        self.db.clone()
    }
}
