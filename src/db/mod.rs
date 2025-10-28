pub(crate) mod cleaner;
pub(crate) mod database;
pub(crate) mod error;
pub(crate) mod logs;
pub(crate) mod migrations;
pub(crate) mod models;
pub(crate) mod tests;

use crate::db::database::Database;
use crate::db::error::DBError;
use crate::db::migrations::run_migrations;
use crate::utils::errors::InternalError;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{ConnectOptions, SqlitePool};
use std::path::Path;
use std::str::FromStr;

const DB_SQLITE: &str = "db.sqlite";

pub async fn open(repository_path: &Path) -> Result<Database, InternalError> {
    let db_path = repository_path.join(DB_SQLITE);
    let pool = establish_connection(db_path.to_str().unwrap()).await?;
    run_migrations(&pool).await?;

    let logs = logs::Logs::new(repository_path).await?;

    let db = Database::new(pool, logs);

    db.clean().await?;

    Ok(db)
}

async fn establish_connection(database_url: &str) -> Result<SqlitePool, DBError> {
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .log_slow_statements(log::LevelFilter::Debug, std::time::Duration::from_secs(30))
        .busy_timeout(std::time::Duration::from_secs(1800))
        .pragma("cache_size", "-1048576")
        .pragma("temp_store", "MEMORY")
        .pragma("synchronous", "NORMAL")
        .pragma("mmap_size", "1000000000")
        .pragma("page_size", "8192")
        .pragma("journal_size_limit", "500000000")
        .pragma("wal_autocheckpoint", "10000")
        .optimize_on_close(true, 10000000);

    SqlitePoolOptions::new()
        .max_connections(5)
        .acquire_timeout(std::time::Duration::from_secs(600))
        .acquire_slow_threshold(std::time::Duration::from_secs(450))
        .connect_with(options)
        .await
        .map_err(DBError::from)
}
