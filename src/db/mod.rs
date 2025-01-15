pub(crate) mod models;
pub(crate) mod schema;
pub(crate) mod db;

use std::str::FromStr;
use sqlx::sqlite::{SqlitePoolOptions, SqliteConnectOptions};
use sqlx::{Error, SqlitePool};

pub async fn establish_connection(database_url: &str) -> Result<SqlitePool, Error> {
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true);
    SqlitePoolOptions::new()
        .max_connections(5) // Adjust based on your needs
        .connect_with(options)
        .await
}
