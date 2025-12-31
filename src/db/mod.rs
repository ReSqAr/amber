pub(crate) mod database;
pub(crate) mod error;
pub(crate) mod kv;
pub(crate) mod models;
pub(crate) mod reduced;
pub(crate) mod stores;
mod versioning;

use crate::db::database::Database;
use crate::db::kv::KVStores;
use crate::utils::errors::InternalError;
use futures::TryFutureExt;
use std::path::Path;

pub async fn open(repository_path: &Path) -> Result<Database, InternalError> {
    let db_path = repository_path.join("db");
    let kv =
        KVStores::new(db_path.clone()).inspect_err(|e| log::error!("Error loading kv: {:?}", e));
    let logs =
        reduced::Reduced::new(db_path).inspect_err(|e| log::error!("Error loading logs: {:?}", e));
    let (kv, logs) = tokio::try_join!(kv, logs)?;
    let db = Database::new(kv, logs);

    db.clean().await?;

    Ok(db)
}
