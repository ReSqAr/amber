use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;

pub(crate) async fn run_sql(path: &Path, sql: String) -> Result<(), anyhow::Error> {
    let path = path.join(".amb/db.sqlite");
    let options = SqliteConnectOptions::from_str(path.to_str().unwrap())?
        .journal_mode(SqliteJournalMode::Wal);

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;

    let result = sqlx::query(&sql).execute(&pool).await?;
    println!("     > rows affected: {}", result.rows_affected());

    Ok(())
}
