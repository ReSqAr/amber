use crate::repository::grpc::GRPCClient;
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::{AppError, InternalError};
use crate::utils::port;
use crate::utils::rclone::{ConfigSection, RCloneTarget};
use base64::Engine;
use cipher::{KeyIvInit, StreamCipher};
use log::{debug, error, warn};
use rand::Rng;
use rand::distr::Alphanumeric;
use russh::client::AuthResult;
use russh::keys::Algorithm;
use russh::keys::agent::client::AgentClient;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub enum SshAuth {
    Password(String),
    Agent,
}

#[derive(Clone, Debug)]
pub struct SshConfig {
    application: String,
    host: String,
    port: Option<u16>,
    user: String,
    auth: SshAuth,
    remote_path: String,
}

impl SshConfig {
    pub(crate) fn from_parameter(parameter: String) -> Result<Self, InternalError> {
        let (user_and_password, remainder) =
            parameter.split_once('@').ok_or_else(|| AppError::Parse {
                message: "missing '@' in SSH connection".into(),
                raw: parameter.clone(),
            })?;

        let (user, auth) = match user_and_password.split_once(':') {
            Some((u, p)) => (u.to_string(), SshAuth::Password(p.to_string())),
            None => (user_and_password.to_string(), SshAuth::Agent),
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

        debug!(
            "parameter={parameter} => user={user}, auth={auth:?}, host={host}, port={port:?} remote_path={remote_path}"
        );

        Ok(Self {
            application: "amber".into(),
            host,
            port,
            user,
            auth,
            remote_path: remote_path.to_string(),
        })
    }

    pub(crate) fn as_rclone_target(&self, remote_path: String) -> SshTarget {
        SshTarget {
            remote_name: rand::rng()
                .sample_iter(&Alphanumeric)
                .take(16)
                .map(char::from)
                .collect(),
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            auth: match self.auth.clone() {
                SshAuth::Password(pw) => SshAuth::Password(pw),
                SshAuth::Agent => SshAuth::Agent,
            },
            remote_path,
        }
    }

    pub(crate) async fn connect(&self) -> Result<WrappedRepository, InternalError> {
        let (tx, mut rx) = mpsc::channel::<Result<SshSetup, InternalError>>(100);
        let ssh_config = self.clone();
        let local_port = port::find_available_port().await?;
        debug!("local_port: {local_port}");

        // dedicated thread for SSH operations
        tokio::spawn(async move {
            let result = setup_app_via_ssh(ssh_config, local_port).await;
            if let Err(e) = tx.send(result).await {
                eprintln!("Failed to send ThreadResponse: {e}");
            }
        });

        let SshSetup {
            thread_response: ThreadResponse { port, auth_key },
            shutdown,
        } = match rx.recv().await {
            Some(Ok(ssh_setup)) => ssh_setup,
            Some(Err(e)) => return Err(e),
            None => {
                return Err(InternalError::Ssh(
                    "Failed to receive ServeInfo".to_string(),
                ));
            }
        };
        debug!("thread_response: port={port} auth_key={auth_key}");

        let addr = format!("http://127.0.0.1:{}", port);
        let repository = GRPCClient::connect(addr, auth_key, shutdown)
            .await
            .map_err(|e| {
                InternalError::Ssh(format!("gRPC connection to port {port} failed: {e}"))
            })?;

        Ok(WrappedRepository::Grpc(repository))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct ServeResponse {
    pub(crate) port: u16,
    pub(crate) auth_key: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct ServeError {
    pub(crate) error: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")] // Uses a "type" field to distinguish variants
pub(crate) enum ServeResult {
    #[serde(rename = "success")]
    Success(ServeResponse),
    #[serde(rename = "error")]
    Error(ServeError),
}

#[derive(Debug, Clone)]
struct ThreadResponse {
    port: u16,
    auth_key: String,
}

pub(crate) type ShutdownFn = Box<dyn Fn() + Send + Sync + 'static>;

struct SshSetup {
    thread_response: ThreadResponse,
    shutdown: ShutdownFn,
}

struct Client;

impl russh::client::Handler for Client {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

async fn setup_app_via_ssh(
    ssh_config: SshConfig,
    local_port: u16,
) -> Result<SshSetup, InternalError> {
    let config = Arc::new(russh::client::Config::default());
    let mut session = russh::client::connect(
        config,
        (ssh_config.host, ssh_config.port.unwrap_or(22)),
        Client,
    )
    .await
    .map_err(|e| InternalError::Ssh(format!("Connection failed: {}", e)))?;

    match ssh_config.auth {
        SshAuth::Password(pwd) => {
            let auth_result = session
                .authenticate_password(&ssh_config.user, &pwd)
                .await
                .map_err(|e| InternalError::Ssh(format!("Authentication failed: {}", e)))?;

            if auth_result != AuthResult::Success {
                return Err(InternalError::Ssh("Authentication failed.".into()));
            }
        }
        SshAuth::Agent => {
            let mut client = AgentClient::connect_env()
                .await
                .map_err(InternalError::RusshKeys)?;
            let identities = client.request_identities().await?;
            let user = ssh_config.user;

            let mut authenticated = false;
            let hash_alg = session.best_supported_rsa_hash().await?.flatten();
            let identity_len = identities.len();
            for pubkey in identities {
                let hash_alg = match pubkey.algorithm() {
                    Algorithm::Dsa | Algorithm::Rsa { .. } => hash_alg,
                    _ => None, // upstream bug
                };

                let auth_result = session
                    .authenticate_publickey_with(&user, pubkey, hash_alg, &mut client)
                    .await
                    .map_err(|e| {
                        InternalError::Ssh(format!("authentication via ssh-agent failed: {}", e))
                    })?;

                if auth_result == AuthResult::Success {
                    authenticated = true;
                    break;
                }
            }

            if !authenticated {
                return Err(InternalError::Ssh(format!(
                    "ssh key authentication failed - unable to authenticate using any of the {} stored identities in ssh-agent",
                    identity_len
                )));
            }
        }
    }

    let remote_command = format!(
        "{} --path \"{}\" serve",
        ssh_config.application, ssh_config.remote_path
    );
    debug!("executing remote command: {}", remote_command);

    let channel = session
        .channel_open_session()
        .await
        .map_err(|e| InternalError::Ssh(format!("Channel open failed: {}", e)))?;

    channel
        .exec(false, remote_command.as_bytes())
        .await
        .map_err(|e| InternalError::Ssh(format!("Command execution failed: {}", e)))?;

    let mut reader = BufReader::new(channel.into_stream());
    let mut buffer = String::new();
    reader
        .read_line(&mut buffer)
        .await
        .map_err(|e| InternalError::Ssh(format!("Failed to read from channel: {}", e)))?;

    let output = String::from_utf8(buffer.into())
        .map_err(|e| InternalError::Ssh(format!("UTF8 error: {}", e)))?;
    debug!("received output: {}", output);

    let serve_response: ServeResult = serde_json::from_str(&output)
        .map_err(|e| InternalError::Ssh(format!("JSON parse error: {}", e)))?;

    let serve_result = match serve_response {
        ServeResult::Success(resp) => resp,
        ServeResult::Error(e) => return Err(InternalError::Ssh(e.error)),
    };

    debug!("parsed ServeResponse: {:?}", serve_result);
    let auth_key = serve_result.auth_key;
    let remote_port = serve_result.port;

    let remote_reader_handle = tokio::spawn(async move {
        loop {
            let mut buffer = String::new();
            if let Err(e) = reader.read_line(&mut buffer).await {
                error!("Failed to read from remote channel: {}", e);
                break;
            }
            debug!("[remote] {buffer}");
        }
    });

    // port forwarding in new asynchronous task
    let port_forward_handle = tokio::spawn(async move {
        let listener = match TcpListener::bind(format!("127.0.0.1:{local_port}")).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("TcpListener::bind: {e}");
                return;
            }
        };

        loop {
            let (mut local_stream, _) = match listener.accept().await {
                Ok(local_stream) => local_stream,
                Err(e) => {
                    error!("listener.accept(): {e}");
                    break;
                }
            };
            let channel = match session
                .channel_open_direct_tcpip(
                    "127.0.0.1",
                    remote_port.into(),
                    "127.0.0.1",
                    local_port.into(),
                )
                .await
            {
                Ok(channel) => channel,
                Err(e) => {
                    error!("session.channel_open_direct_tcpip(): {e}");
                    break;
                }
            };

            tokio::spawn(async move {
                let mut remote_stream = channel.into_stream();
                match tokio::io::copy_bidirectional(&mut local_stream, &mut remote_stream).await {
                    Ok((u, d)) => debug!("up: {u} bytes down: {d} bytes"),
                    Err(e) => warn!("tokio::io::copy_bidirectional: {e}"),
                };
            });
        }
    });

    Ok(SshSetup {
        thread_response: ThreadResponse {
            port: local_port,
            auth_key,
        },
        shutdown: Box::new(move || {
            remote_reader_handle.abort();
            port_forward_handle.abort();
        }),
    })
}

#[derive(Debug, Clone)]
pub struct SshTarget {
    pub remote_name: String,
    pub host: String,
    pub port: Option<u16>,
    pub user: String,
    pub auth: SshAuth,
    pub remote_path: String,
}

impl RCloneTarget for SshTarget {
    fn to_rclone_arg(&self) -> String {
        format!("{}:{}", self.remote_name, self.remote_path)
    }

    fn to_config_section(&self) -> ConfigSection {
        let SshTarget {
            remote_name,
            host,
            port,
            user,
            auth,
            ..
        } = self;
        let mut lines = vec![
            format!("[{remote_name}]"),
            "type = sftp".into(),
            format!("host = {host}"),
            format!("user = {user}"),
        ];
        if let Some(port) = port {
            lines.push(format!("port = {port}"));
        }

        match auth {
            SshAuth::Password(password) => {
                lines.push(format!("pass = {}", rclone_obscure_password(password)))
            }
            SshAuth::Agent => lines.push("key_use_agent = true".into()),
        }

        lines.push("".into());
        ConfigSection::Config(lines.join("\n"))
    }
}

const RCLONE_KEY: [u8; 32] = [
    0x9c, 0x93, 0x5b, 0x48, 0x73, 0x0a, 0x55, 0x4d, 0x6b, 0xfd, 0x7c, 0x63, 0xc8, 0x86, 0xa9, 0x2b,
    0xd3, 0x90, 0x19, 0x8e, 0xb8, 0x12, 0x8a, 0xfb, 0xf4, 0xde, 0x16, 0x2b, 0x8b, 0x95, 0xf6, 0x38,
];

type Aes256Ctr = ctr::Ctr128BE<aes::Aes256>;

fn rclone_obscure_password(input: &str) -> String {
    if input.is_empty() {
        return "".to_string();
    }
    let iv = [0u8; 16];
    let mut buffer = Vec::with_capacity(iv.len() + input.len());
    buffer.extend_from_slice(&iv);
    buffer.extend_from_slice(input.as_bytes());
    let mut cipher = Aes256Ctr::new(&RCLONE_KEY.into(), &iv.into());
    cipher.apply_keystream(&mut buffer[16..]);
    let engine = base64::engine::GeneralPurpose::new(
        &base64::alphabet::URL_SAFE,
        base64::engine::general_purpose::NO_PAD,
    );
    engine.encode(&buffer)
}
