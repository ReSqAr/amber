pub(crate) mod database;
pub(crate) mod error;
pub(crate) mod migrations;
pub(crate) mod models;
pub(crate) mod tests;

use crate::db::error::DBError;
use database::Pool;

pub async fn establish_connection(database_url: &str) -> Result<Pool, DBError> {
    let pool = Pool::new(database_url)?;
    migrations::run_migrations(&pool).await?;
    Ok(pool)
}
