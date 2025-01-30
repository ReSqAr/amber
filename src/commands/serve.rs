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
use std::sync::Arc;
use tokio::io;
use tokio::io::AsyncReadExt;
use tokio::signal::unix::{signal, SignalKind};
use tonic::transport::Server;
use tonic::{Request, Status};

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

#[derive(Clone)]
pub struct AuthInterceptor {
    auth_key: Arc<String>,
}

impl AuthInterceptor {
    pub fn new(auth_key: String) -> Self {
        Self {
            auth_key: Arc::new(auth_key),
        }
    }
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        match request.metadata().get("authorization") {
            Some(auth_token) => match auth_token.to_str() {
                Ok(token) => {
                    if token == self.auth_key.as_str() {
                        Ok(request)
                    } else {
                        Err(Status::unauthenticated("Invalid authorization token"))
                    }
                }
                Err(_) => Err(Status::unauthenticated(
                    "Invalid authorization token format",
                )),
            },
            None => Err(Status::unauthenticated("Missing authorization token")),
        }
    }
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

    // Create a future that listens for SIGHUP signals
    let shutdown_signal = async {
        let mut sighup = signal(SignalKind::hangup()).expect("SIGHUP handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
        let mut stdin = io::stdin();
        let mut eof_buffer = [0u8; 1];

        tokio::select! {
            _ = sighup.recv() => debug!("received SIGHUP"),
            _ = sigterm.recv() => debug!("received SIGTERM"),
            _ = sigint.recv() => debug!("received SIGINT"),
            _ = stdin.read(&mut eof_buffer) => debug!("stdin closed"),
        };

        debug!("initiating graceful shutdown");
    };

    debug!("listening on {}", addr);
    let auth_interceptor = AuthInterceptor::new(auth_key);
    let service = Service::new(local_repository);
    let service = definitions::grpc_server::GrpcServer::with_interceptor(service, auth_interceptor);
    let server = Server::builder()
        .add_service(service)
        .serve_with_shutdown(addr, shutdown_signal);

    server.await?;

    Ok(())
}
