pub(crate) mod database;
pub(crate) mod migrations;
pub(crate) mod models;
pub(crate) mod tests;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{ConnectOptions, Error, SqlitePool};
use std::str::FromStr;

pub async fn establish_connection(database_url: &str) -> Result<SqlitePool, Error> {
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .log_slow_statements(log::LevelFilter::Off, std::time::Duration::from_secs(30))
        .busy_timeout(std::time::Duration::from_secs(60)); // TODO: auto optimise at the end
    SqlitePoolOptions::new()
        .max_connections(5) // Adjust based on your needs
        .connect_with(options)
        .await
}
