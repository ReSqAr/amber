pub(crate) mod database;
pub(crate) mod error;
pub(crate) mod kv;
pub(crate) mod logs;
pub(crate) mod models;
pub(crate) mod scratch;

use crate::db::database::Database;
use crate::db::kv::KVStores;
use crate::utils::errors::InternalError;
use std::path::Path;

pub async fn open(repository_path: &Path) -> Result<Database, InternalError> {
    let logs = logs::Logs::new(repository_path).await?;
    let redb = KVStores::new(&repository_path.join("redb")).await?;
    let db = Database::new(redb, logs);

    db.clean().await?;

    Ok(db)
}
