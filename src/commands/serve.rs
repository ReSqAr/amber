use crate::commands::errors::InvariableError;
use crate::db::db::DB;
use crate::db::establish_connection;
use crate::db::schema::run_migrations;
use crate::transport::server::{invariable, MyServer};
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::fs;
use tonic::transport::Server;

pub async fn serve(port: u16) -> Result<(), Box<dyn std::error::Error>> {
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

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let server = MyServer { db, invariable_path };

    info!("listening on {}", addr);

    Server::builder()
        .add_service(invariable::invariable_server::InvariableServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
