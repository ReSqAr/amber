use sqlx::migrate::MigrateError;
use sqlx::SqlitePool;

pub async fn run_migrations(pool: &SqlitePool) -> Result<(), MigrateError> {
    sqlx::migrate!("./migrations").run(pool).await
}
