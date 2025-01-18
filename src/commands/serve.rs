use crate::commands::errors::InvariableError;
use crate::db::db::DB;
use crate::db::establish_connection;
use crate::db::schema::run_migrations;
use crate::repository::local_repository::LocalRepository;
use crate::transport::server::{invariable, MyServer};
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::fs;
use tonic::transport::Server;

pub async fn serve(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let server = MyServer {
        repository: local_repository,
    };

    info!("listening on {}", addr);

    Server::builder()
        .add_service(invariable::invariable_server::InvariableServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
