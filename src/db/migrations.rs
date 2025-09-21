use crate::db::error::DBError;
use sqlx::SqlitePool;

pub async fn run_migrations(pool: &SqlitePool) -> Result<(), DBError> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .map_err(DBError::from)
}
