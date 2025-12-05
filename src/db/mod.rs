pub(crate) mod database;
pub(crate) mod error;
pub(crate) mod kv;
pub(crate) mod kvstore;
pub(crate) mod logs;
pub(crate) mod models;

use crate::db::database::Database;
use crate::db::kv::KVStores;
use crate::utils::errors::InternalError;
use std::path::Path;

pub async fn open(repository_path: &Path) -> Result<Database, InternalError> {
    let kv = KVStores::new(repository_path.join("kv"));
    let logs = logs::Logs::new(repository_path);
    let (kv, logs) = tokio::try_join!(kv, logs)?;
    let db = Database::new(kv, logs);

    db.clean().await?;

    Ok(db)
}
