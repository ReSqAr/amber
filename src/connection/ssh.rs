use crate::repository::grpc::GRPCClient;
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::{AppError, InternalError};
use crate::utils::port;
use crate::utils::rclone::{ConfigSection, RCloneTarget};
use base64::Engine;
use cipher::{KeyIvInit, StreamCipher};
use log::{debug, error, info};
use rand::RngExt;
use rand::distr::Alphanumeric;
use russh::client::AuthResult;
use russh::keys::agent::client::AgentClient;
use russh::keys::known_hosts::{check_known_hosts, check_known_hosts_path};
use russh::keys::{Algorithm, PublicKeyOrCertificate};
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
    #[allow(clippy::result_large_err)]
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
                log::error!("Failed to send ThreadResponse: {e}");
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

/// Outcome of matching a server key against the user's `known_hosts`.
#[derive(Debug)]
pub(crate) enum HostKeyVerdict {
    /// The key is recorded for this host and matches.
    Accepted,
    /// The host has no entry - first contact, or the wrong host.
    Unknown { fingerprint: String },
    /// An entry exists and the key does not match it.
    Changed { line: usize, fingerprint: String },
    /// `known_hosts` could not be consulted at all.
    Unreadable { error: String },
    /// Host certificates are not supported.
    UnsupportedCertificate,
}

impl HostKeyVerdict {
    fn describe(&self, host: &str, port: u16) -> String {
        match self {
            HostKeyVerdict::Accepted => format!("host key for {host}:{port} is known"),
            HostKeyVerdict::Unknown { fingerprint } => format!(
                "the host key of {host}:{port} ({fingerprint}) is not in your known_hosts file. \
                 Connect once with `ssh -p {port} {host}` to record it, then retry"
            ),
            HostKeyVerdict::Changed { line, fingerprint } => format!(
                "REMOTE HOST IDENTIFICATION HAS CHANGED: the host key of {host}:{port} \
                 ({fingerprint}) does not match the entry on line {line} of your known_hosts \
                 file. Someone could be eavesdropping on you right now. If the host key was \
                 changed legitimately, remove that line and record the new key"
            ),
            HostKeyVerdict::Unreadable { error } => {
                format!("unable to check the host key of {host}:{port}: {error}")
            }
            HostKeyVerdict::UnsupportedCertificate => format!(
                "{host}:{port} authenticated with a host certificate, which amber cannot verify"
            ),
        }
    }
}

/// Overrides the `known_hosts` file to consult, like ssh's `UserKnownHostsFile`.
pub(crate) const KNOWN_HOSTS_ENV: &str = "AMBER_SSH_KNOWN_HOSTS";

static KNOWN_HOSTS_PATH: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

/// Sets the `known_hosts` file programmatically; takes precedence over the
/// environment variable and can only be set once, before the first connection.
pub(crate) fn set_known_hosts_path(path: std::path::PathBuf) {
    let _ = KNOWN_HOSTS_PATH.set(path);
}

fn known_hosts_override() -> Option<std::path::PathBuf> {
    if let Some(path) = KNOWN_HOSTS_PATH.get() {
        return Some(path.clone());
    }
    std::env::var_os(KNOWN_HOSTS_ENV).map(std::path::PathBuf::from)
}

/// Checks a server key against `known_hosts` - the user's own file, or `path`
/// when one is given.
pub(crate) fn verify_host_key(
    host: &str,
    port: u16,
    key: &PublicKeyOrCertificate,
    path: Option<&std::path::Path>,
) -> HostKeyVerdict {
    let key = match key {
        PublicKeyOrCertificate::PublicKey { key, .. } => key,
        PublicKeyOrCertificate::Certificate(_) => {
            return HostKeyVerdict::UnsupportedCertificate;
        }
    };
    let fingerprint = key.fingerprint(Default::default()).to_string();

    let known = match path {
        Some(path) => check_known_hosts_path(host, port, key, path),
        None => check_known_hosts(host, port, key),
    };

    match known {
        Ok(true) => HostKeyVerdict::Accepted,
        Ok(false) => HostKeyVerdict::Unknown { fingerprint },
        Err(russh::keys::Error::KeyChanged { line }) => {
            HostKeyVerdict::Changed { line, fingerprint }
        }
        Err(e) => HostKeyVerdict::Unreadable {
            error: e.to_string(),
        },
    }
}

/// Records why a host key was rejected, so the connection error can say more
/// than "unknown key".
type RejectionSlot = Arc<std::sync::Mutex<Option<String>>>;

struct Client {
    host: String,
    port: u16,
    rejection: RejectionSlot,
}

impl russh::client::Handler for Client {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        let verdict = verify_host_key(
            &self.host,
            self.port,
            server_public_key,
            known_hosts_override().as_deref(),
        );
        let message = verdict.describe(&self.host, self.port);
        match verdict {
            HostKeyVerdict::Accepted => {
                debug!("{message}");
                Ok(true)
            }
            HostKeyVerdict::Unknown { .. }
            | HostKeyVerdict::Changed { .. }
            | HostKeyVerdict::Unreadable { .. }
            | HostKeyVerdict::UnsupportedCertificate => {
                error!("{message}");
                if let Ok(mut slot) = self.rejection.lock() {
                    *slot = Some(message);
                }
                Ok(false)
            }
        }
    }
}

async fn setup_app_via_ssh(
    ssh_config: SshConfig,
    local_port: u16,
) -> Result<SshSetup, InternalError> {
    let config = Arc::new(russh::client::Config::default());
    let port = ssh_config.port.unwrap_or(22);
    let rejection: RejectionSlot = Arc::new(std::sync::Mutex::new(None));
    let handler = Client {
        host: ssh_config.host.clone(),
        port,
        rejection: rejection.clone(),
    };
    let mut session = russh::client::connect(config, (ssh_config.host, port), handler)
        .await
        .map_err(|e| {
            // A rejected host key surfaces as a generic protocol error, so report
            // the reason the handler recorded instead.
            match rejection.lock().ok().and_then(|mut slot| slot.take()) {
                Some(reason) => InternalError::Ssh(reason),
                None => InternalError::Ssh(format!("Connection failed: {}", e)),
            }
        })?;

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
            for identity in identities {
                let pubkey = identity.public_key().into_owned();
                let hash_alg = match pubkey.algorithm() {
                    Algorithm::Dsa | Algorithm::Rsa { .. } => hash_alg,
                    Algorithm::Ecdsa { .. }
                    | Algorithm::Ed25519
                    | Algorithm::SkEcdsaSha2NistP256
                    | Algorithm::SkEd25519
                    | Algorithm::Other(_) => None,
                    _ => None,
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

    let output = buffer;
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
            match reader.read_line(&mut buffer).await {
                // End of the remote channel - without this the loop spins.
                Ok(0) => {
                    debug!("remote channel closed");
                    break;
                }
                Ok(_) => debug!("[remote] {buffer}"),
                Err(e) => {
                    error!("Failed to read from remote channel: {}", e);
                    break;
                }
            }
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
                    Err(e) => info!("tokio::io::copy_bidirectional: {e}"),
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
    // rclone prepends a random IV to the ciphertext and reads it back when
    // deobscuring. A fixed IV would reuse the same keystream for every password.
    let mut iv = [0u8; 16];
    rand::rng().fill(&mut iv[..]);
    let mut buffer = Vec::with_capacity(iv.len() + input.len());
    buffer.extend_from_slice(&iv);
    buffer.extend_from_slice(input.as_bytes());
    let mut cipher = Aes256Ctr::new(&RCLONE_KEY.into(), &iv.into());
    #[allow(clippy::indexing_slicing)]
    cipher.apply_keystream(&mut buffer[16..]);
    let engine = base64::engine::GeneralPurpose::new(
        &base64::alphabet::URL_SAFE,
        base64::engine::general_purpose::NO_PAD,
    );
    engine.encode(&buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use russh::keys::PrivateKey;
    use std::io::Write as _;

    fn key_pair() -> PrivateKey {
        PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519).expect("generate key")
    }

    fn known_hosts_file(entries: &[(&str, &PrivateKey)]) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        for (host, key) in entries {
            let public = key.public_key().to_openssh().expect("openssh encoding");
            writeln!(file, "{host} {public}").expect("write entry");
        }
        file.flush().expect("flush");
        file
    }

    fn public(key: &PrivateKey) -> PublicKeyOrCertificate {
        PublicKeyOrCertificate::PublicKey {
            key: key.public_key().clone(),
            hash_alg: None,
        }
    }

    #[test]
    fn a_recorded_host_key_is_accepted() {
        let key = key_pair();
        let file = known_hosts_file(&[("[tycho.com]:2222", &key)]);

        let verdict = verify_host_key("tycho.com", 2222, &public(&key), Some(file.path()));
        assert!(
            matches!(verdict, HostKeyVerdict::Accepted),
            "expected the recorded key to be accepted, got {verdict:?}"
        );
    }

    #[test]
    fn an_unrecorded_host_key_is_rejected() {
        let key = key_pair();
        let file = known_hosts_file(&[]);

        let verdict = verify_host_key("tycho.com", 2222, &public(&key), Some(file.path()));
        assert!(
            matches!(verdict, HostKeyVerdict::Unknown { .. }),
            "expected an unknown host key, got {verdict:?}"
        );
    }

    /// The case that matters: the host is known, and answers with another key.
    #[test]
    fn a_changed_host_key_is_rejected() {
        let recorded = key_pair();
        let impostor = key_pair();
        let file = known_hosts_file(&[("[tycho.com]:2222", &recorded)]);

        let verdict = verify_host_key("tycho.com", 2222, &public(&impostor), Some(file.path()));
        assert!(
            matches!(verdict, HostKeyVerdict::Changed { .. }),
            "expected a changed host key, got {verdict:?}"
        );
    }

    #[test]
    fn a_key_recorded_for_another_host_is_rejected() {
        let key = key_pair();
        let file = known_hosts_file(&[("[medina.com]:2222", &key)]);

        let verdict = verify_host_key("tycho.com", 2222, &public(&key), Some(file.path()));
        assert!(
            matches!(verdict, HostKeyVerdict::Unknown { .. }),
            "expected an unknown host key, got {verdict:?}"
        );
    }

    /// rclone reads the IV back from the first 16 bytes, so a random IV round
    /// trips just as well - and does not reuse the keystream.
    #[test]
    fn obscured_passwords_use_a_fresh_iv() {
        let first = rclone_obscure_password("hunter2");
        let second = rclone_obscure_password("hunter2");
        assert_ne!(
            first, second,
            "the same password must not obscure to the same value twice"
        );

        for obscured in [first, second] {
            let engine = base64::engine::GeneralPurpose::new(
                &base64::alphabet::URL_SAFE,
                base64::engine::general_purpose::NO_PAD,
            );
            let mut buffer = engine.decode(obscured).expect("decode");
            assert!(buffer.len() > 16);
            let (iv, ciphertext) = buffer.split_at_mut(16);
            let iv: [u8; 16] = iv.try_into().expect("iv");
            let mut cipher = Aes256Ctr::new(&RCLONE_KEY.into(), &iv.into());
            cipher.apply_keystream(ciphertext);
            assert_eq!(std::str::from_utf8(ciphertext).expect("utf8"), "hunter2");
        }
    }

    #[test]
    fn an_empty_password_stays_empty() {
        assert_eq!(rclone_obscure_password(""), "");
    }
}
