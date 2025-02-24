use sqlx::SqlitePool;
use sqlx::migrate::MigrateError;

pub async fn run_migrations(pool: &SqlitePool) -> Result<(), MigrateError> {
    sqlx::migrate!("./migrations").run(pool).await
}
