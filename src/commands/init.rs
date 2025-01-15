use std::io::Error;
use tokio::fs;
use crate::db::db::DB;
use crate::db::establish_connection;
use crate::db::schema::run_migrations;

pub async fn init_repository() -> Result<(), Error> {
    let current_path = fs::canonicalize(".").await?;
    let invariable_path = current_path.join(".inv");
    if fs::metadata(invariable_path.as_path())
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        return Err(Error::other("oh no! already exists"));
    }

    fs::create_dir(invariable_path.as_path()).await?;

    let db_path = invariable_path.join("db.sqlite");
    let pool = establish_connection(db_path.to_str().unwrap())
        .await
        .expect("failed to establish connection");
    run_migrations(&pool)
        .await
        .expect("failed to run migrations");

    let db = DB::new(pool.clone());
    let repo = db
        .get_or_create_current_repository()
        .await
        .expect("failed to create repo id");
    println!("Initialised repository id={} in {}", repo.repo_id, current_path.display());

    Ok(())
}