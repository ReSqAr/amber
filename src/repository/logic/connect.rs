use crate::db::models::ConnectionType;
use crate::repository::grpc::GRPCClient;
use crate::repository::local::LocalRepository;
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::{AppError, InternalError};
use crate::utils::{rclone, ssh};
use log::debug;
use serde::Deserialize;
use ssh2::Session;
use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

#[derive(Clone, Debug)]
pub struct LocalConfig {
    root: String,
}

impl LocalConfig {
    pub(crate) fn from_parameter(parameter: String) -> Result<Self, InternalError> {
        Ok(Self { root: parameter })
    }

    pub(crate) fn as_rclone_target(&self) -> rclone::RcloneTarget {
        rclone::RcloneTarget::Local(rclone::LocalConfig {
            path: self.root.clone().into(),
        })
    }

    pub(crate) async fn connect(&self) -> Result<WrappedRepository, InternalError> {
        let LocalConfig { root } = self;
        let repository = LocalRepository::new(Some(root.clone().into())).await?;
        Ok(WrappedRepository::Local(repository))
    }
}

#[derive(Clone, Debug)]
pub struct SshConfig {
    application: String,
    host: String,
    port: Option<u16>,
    user: String,
    password: Option<String>,
    remote_path: String,
}

impl SshConfig {
    pub(crate) fn from_parameter(parameter: String) -> Result<Self, InternalError> {
        let (user_and_password, remainder) =
            parameter.split_once('@').ok_or_else(|| AppError::Parse {
                message: "missing '@' in SSH connection".into(),
                raw: parameter.clone(),
            })?;

        let (user, password) = match user_and_password.split_once(':') {
            Some((u, p)) => (u.to_string(), Some(p.to_string())),
            None => (user_and_password.to_string(), None),
        };

        let slash_pos = remainder.find('/').ok_or_else(|| AppError::Parse {
            message: "missing '/' in SSH connection".into(),
            raw: parameter.clone(),
        })?;
        let (host_part, remote_path) = remainder.split_at(slash_pos);

        let (host, port) = match host_part.split_once(':') {
            Some((h, port_str)) => {
                let port = port_str.parse::<u16>().map_err(|_| AppError::Parse {
                    message: "invalid port".into(),
                    raw: parameter.clone(),
                })?;
                (h.to_string(), Some(port))
            }
            None => (host_part.to_string(), None),
        };

        debug!("parameter={parameter} => user={user}, password={password:?}, host={host}, port={port:?} remote_path={remote_path}");

        Ok(Self {
            application: "amber".into(),
            host,
            port,
            user,
            password,
            remote_path: remote_path.to_string(),
        })
    }

    pub(crate) fn as_rclone_target(&self) -> rclone::RcloneTarget {
        rclone::RcloneTarget::Ssh(rclone::SshConfig {
            remote_name: "remote".into(),
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            remote_path: self.remote_path.clone().into(),
        })
    }

    pub(crate) async fn connect(&self) -> Result<WrappedRepository, InternalError> {
        // Create a channel for communication between threads
        let (tx, rx) = mpsc::channel::<Result<ThreadResponse, InternalError>>();

        // Clone necessary fields for the thread
        let ssh_config = self.clone();

        // Spawn a dedicated thread for SSH operations
        thread::spawn(move || {
            let result = setup_app_via_ssh(ssh_config);
            debug!("result to be sent: {:?}", result);
            if let Err(e) = tx.send(result) {
                eprintln!("Failed to send ThreadResponse: {}", e);
                // Optionally handle the error further if needed
            }
        });

        // Wait for the ThreadResponse from the channel
        let thread_response = match rx.recv() {
            Ok(Ok(info)) => info,
            Ok(Err(e)) => return Err(e),
            Err(e) => {
                return Err(InternalError::Ssh(format!(
                    "Failed to receive ServeInfo: {}",
                    e
                )))
            }
        };
        debug!("thread_response={:?}", thread_response);

        let addr = format!("http://127.0.0.1:{}", thread_response.port);
        let repository = GRPCClient::connect(addr)
            .await
            .map_err(|e| InternalError::Ssh(format!("gRPC connection failed: {}", e)))?;

        Ok(WrappedRepository::Grpc(repository))
    }
}

pub(crate) async fn find_available_port() -> Result<u16, InternalError> {
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?.port())
}

#[derive(Deserialize, Debug, Clone)]
struct ServeResponse {
    port: u16,
    auth_key: String,
}

#[derive(Debug, Clone)]
struct ThreadResponse {
    port: u16,
    auth_key: String,
}

fn setup_app_via_ssh(ssh_config: SshConfig) -> Result<ThreadResponse, InternalError> {
    let tcp_addr = format!("{}:{}", ssh_config.host, ssh_config.port.unwrap_or(22));
    let tcp = TcpStream::connect(&tcp_addr)
        .map_err(|e| InternalError::Ssh(format!("failed to connect: {}", e)))?;

    let mut session = Session::new()
        .map_err(|e| InternalError::Ssh(format!("failed to create session: {}", e)))?;

    session.set_tcp_stream(tcp.try_clone()?);
    session
        .handshake()
        .map_err(|e| InternalError::Ssh(format!("SSH handshake failed: {}", e)))?;

    // Authenticate
    if let Some(pwd) = ssh_config.password {
        session
            .userauth_password(&ssh_config.user, &pwd)
            .map_err(|e| InternalError::Ssh(format!("SSH authentication failed: {}", e)))?;
    }

    if !session.authenticated() {
        return Err(InternalError::Ssh(
            "SSH authentication not completed.".into(),
        ));
    }

    // Run remote "serve" command
    let mut channel = session
        .channel_session()
        .map_err(|e| InternalError::Ssh(format!("failed to open channel: {}", e)))?;

    let remote_command = format!(
        "{} --path \"{}\" serve",
        ssh_config.application, ssh_config.remote_path
    );

    channel
        .exec(&remote_command) // TODO: cleanup after exit
        .map_err(|e| InternalError::Ssh(format!("failed to exec remote command: {}", e)))?;

    let mut stdout = channel.stream(0);
    let mut reader = BufReader::new(&mut stdout);
    let mut line = String::new();

    reader
        .read_line(&mut line)
        .map_err(|e| InternalError::Ssh(format!("failed to read line: {}", e)))?;

    let serve_response: ServeResponse = serde_json::from_str(&line)
        .map_err(|e| InternalError::Ssh(format!("failed to parse JSON: {}", e)))?;

    debug!("ServeResponse: {:?}", serve_response);

    // Prepare the ThreadResponse
    let local_port = 4242; // TODO
    let thread_response = ThreadResponse {
        port: local_port,
        auth_key: serve_response.auth_key.clone(),
    };

    // Create the tunnel
    thread::spawn(move || {
        let _channel = channel;
        ssh::port_forward(
            Arc::new(Mutex::new(session)),
            tcp.into(),
            "127.0.0.1".into(),
            local_port,
            "127.0.0.1".into(),
            serve_response.port,
        )
        .map_err(|e| InternalError::Ssh(format!("Failed to create tunnel: {}", e)))
        .expect("no error");
    });
    debug!("finishing setup");

    Ok(thread_response)
}

#[derive(Clone, Debug)]
pub enum ConnectionConfig {
    Local(LocalConfig),
    Ssh(SshConfig),
}

impl ConnectionConfig {
    pub(crate) fn as_rclone_target(&self) -> rclone::RcloneTarget {
        match self {
            ConnectionConfig::Local(local_config) => local_config.as_rclone_target(),
            ConnectionConfig::Ssh(ssh_config) => ssh_config.as_rclone_target(),
        }
    }
}

pub fn parse_config(
    connection_type: ConnectionType,
    parameter: String,
) -> Result<ConnectionConfig, InternalError> {
    match connection_type {
        ConnectionType::Local => Ok(ConnectionConfig::Local(LocalConfig::from_parameter(
            parameter,
        )?)),
        ConnectionType::Ssh => Ok(ConnectionConfig::Ssh(SshConfig::from_parameter(parameter)?)),
    }
}

pub async fn connect(config: &ConnectionConfig) -> Result<WrappedRepository, InternalError> {
    match config.clone() {
        ConnectionConfig::Local(local_config) => local_config.connect().await,
        ConnectionConfig::Ssh(ssh_config) => ssh_config.connect().await,
    }
}
