use crate::grpc::definitions;
use crate::grpc::service::Service;
use crate::repository::local::LocalRepository;
use crate::repository::logic::connect;
use log::debug;
use rand::distr::Alphanumeric;
use rand::Rng;
use serde::Serialize;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use tonic::transport::Server;

#[derive(Serialize)]
struct ServeReport {
    port: u16,
    auth_key: String,
}

pub fn generate_auth_key() -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(128)
        .map(char::from)
        .collect()
}

pub async fn serve(maybe_root: Option<PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(maybe_root).await?;

    let auth_key = generate_auth_key();
    let port = connect::find_available_port().await?;

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let report = ServeReport {
        port,
        auth_key: auth_key.clone(),
    };
    let json = serde_json::to_string(&report)?;
    println!("{}", json);
    std::io::stdout().flush()?;

    debug!("listening on {}", addr);

    let service = Service::new(local_repository);
    let server = Server::builder()
        .add_service(definitions::grpc_server::GrpcServer::new(service))
        .serve(addr);
    server.await?;

    Ok(())
}
