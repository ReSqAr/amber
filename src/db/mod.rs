pub(crate) mod database;
pub(crate) mod migrations;
pub(crate) mod models;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteJournalMode};
use sqlx::{Error, SqlitePool};
use std::str::FromStr;

pub async fn establish_connection(database_url: &str) -> Result<SqlitePool, Error> {
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal);
    SqlitePoolOptions::new()
        .max_connections(5) // Adjust based on your needs
        .connect_with(options)
        .await
}
