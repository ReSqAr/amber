use crate::commands::errors::InvariableError;
use crate::db::db::DB;
use crate::db::establish_connection;
use crate::db::schema::run_migrations;
use crate::transport::server::invariable::invariable_client::InvariableClient;
use crate::transport::server::invariable::RepositoryIdRequest;
use tokio::fs;

pub async fn sync(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let current_path = fs::canonicalize(".").await?;
    let invariable_path = current_path.join(".inv");
    if !fs::metadata(&invariable_path)
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        return Err(InvariableError::NotInitialised().into());
    };

    let db_path = invariable_path.join("db.sqlite");
    let pool = establish_connection(db_path.to_str().unwrap())
        .await
        .expect("failed to establish connection");
    run_migrations(&pool)
        .await
        .expect("failed to run migrations");

    let db = DB::new(pool.clone());

    let addr = format!("http://127.0.0.1:{}", port);
    let mut client = InvariableClient::connect(addr).await?;

    let request = tonic::Request::new(RepositoryIdRequest {
    });

    let response = client.repository_id(request).await?;

    let msg = response.into_inner();

    println!("repo_id={}", msg.repo_id);

    Ok(())
}