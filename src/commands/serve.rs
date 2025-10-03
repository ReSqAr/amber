use crate::connection::ssh::{ServeError, ServeResponse, ServeResult};
use crate::grpc::auth::ServerAuth;
use crate::grpc::definitions;
use crate::grpc::service::Service;
use crate::logic::files;
use crate::repository::local::{LocalRepository, LocalRepositoryConfig};
use crate::repository::traits::Local;
use crate::utils::errors::InternalError;
use crate::utils::fs::Capability;
use crate::utils::port;
use log::debug;
use rand::Rng;
use rand::distr::Alphanumeric;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use tokio::io;
use tokio::io::AsyncReadExt;
use tokio::signal::unix::{SignalKind, signal};
use tonic::transport::Server;

pub fn generate_auth_key() -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(128)
        .map(char::from)
        .collect()
}

pub async fn serve(
    config: LocalRepositoryConfig,
    output: crate::flightdeck::output::Output,
) -> Result<(), InternalError> {
    let auth_key = generate_auth_key();
    let port = port::find_available_port().await?;

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
        }

        debug!("initiating graceful shutdown");
    };

    serve_on_port(
        config.maybe_root,
        config.app_folder,
        config.preferred_capability,
        output,
        port,
        auth_key,
        shutdown_signal,
    )
    .await
}

pub async fn serve_on_port(
    maybe_root: Option<PathBuf>,
    app_folder: PathBuf,
    preferred_capability: Option<Capability>,
    output: crate::flightdeck::output::Output,
    port: u16,
    auth_key: String,
    shutdown_signal: impl Future<Output = ()>,
) -> Result<(), InternalError> {
    let config = LocalRepositoryConfig {
        maybe_root,
        app_folder,
        preferred_capability,
    };

    let local_repository = match LocalRepository::new(config).await {
        Ok(local_repository) => local_repository,
        Err(e) => {
            let error = ServeResult::Error(ServeError {
                error: e.to_string(),
            });
            let json =
                serde_json::to_string(&error).map_err(|e| InternalError::SerialisationError {
                    object: format!("{error:?}"),
                    e: e.to_string(),
                })?;
            output.println(json.to_string());
            return Err(e);
        }
    };

    let staging_path = local_repository.staging_path();

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let report = ServeResult::Success(ServeResponse {
        port,
        auth_key: auth_key.clone(),
    });
    let json = serde_json::to_string(&report).map_err(|e| InternalError::SerialisationError {
        object: format!("{report:?}"),
        e: e.to_string(),
    })?;
    output.println(json.to_string());
    std::io::stdout().flush()?;

    debug!("listening on {}", addr);
    let auth_interceptor = ServerAuth::new(auth_key);
    let service = Service::new(local_repository);
    let service = definitions::grpc_server::GrpcServer::with_interceptor(service, auth_interceptor);
    let server = Server::builder()
        .add_service(service)
        .serve_with_shutdown(addr, shutdown_signal);

    server.await?;

    files::cleanup_staging(&staging_path).await?;
    debug!("deleted staging {}", staging_path.display());

    Ok(())
}
