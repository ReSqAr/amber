use crate::grpc::server::{grpc, GRPCServer};
use crate::repository::local_repository::LocalRepository;
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tonic::transport::Server;

pub async fn serve(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let server = GRPCServer::new(local_repository);

    info!("listening on {}", addr);

    Server::builder()
        .add_service(grpc::grpc_server::GrpcServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
