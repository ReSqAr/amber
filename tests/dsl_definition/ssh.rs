use crate::dsl_definition::writer::ChannelWriter;
use amber::commands::serve;
use amber::flightdeck::output::Output;
use cipher::crypto_common::rand_core::OsRng;
use russh::keys::{Algorithm, PrivateKey};
use russh::server::{Auth, Handler, Server, Session};
use russh::{Channel, ChannelId};
use russh_sftp::protocol::{
    Attrs, Data, File, FileAttributes, Handle, Name, OpenFlags, Status, StatusCode, Version,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio::fs;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;

// SSH Server Implementation
#[derive(Clone)]
struct SshServer {
    password: String,
    repo_path: PathBuf,
    ssh_clients:
        std::sync::Arc<tokio::sync::Mutex<HashMap<(usize, ChannelId), russh::server::Handle>>>,
    client_id: usize,
    server_response: ServeResponse,
}

impl Server for SshServer {
    type Handler = SshSession;

    fn new_client(&mut self, _addr: Option<std::net::SocketAddr>) -> Self::Handler {
        let client_id = self.client_id;
        self.client_id += 1;

        SshSession {
            client_id,
            password: self.password.clone(),
            repo_path: self.repo_path.clone(),
            ssh_clients: self.ssh_clients.clone(),
            server_response: self.server_response.clone(),
            channels: Default::default(),
        }
    }
}

struct SshSession {
    client_id: usize,
    password: String,
    repo_path: PathBuf,
    ssh_clients:
        std::sync::Arc<tokio::sync::Mutex<HashMap<(usize, ChannelId), russh::server::Handle>>>,
    channels: std::sync::Arc<tokio::sync::Mutex<HashMap<ChannelId, Channel<russh::server::Msg>>>>,
    server_response: ServeResponse,
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
#[serde(tag = "type")]
pub(crate) enum ServeResult {
    #[serde(rename = "success")]
    Success(ServeResponse),
    #[serde(rename = "error")]
    Error(ServeError),
}

pub(crate) async fn find_available_port() -> Result<u16, anyhow::Error> {
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?.port())
}

impl Handler for SshSession {
    type Error = anyhow::Error;

    async fn auth_password(
        &mut self,
        _user: &str,
        password: &str,
    ) -> anyhow::Result<Auth, Self::Error> {
        if password == self.password {
            Ok(Auth::Accept)
        } else {
            Ok(Auth::Reject {
                proceed_with_methods: None,
            })
        }
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<russh::server::Msg>,
        session: &mut Session,
    ) -> anyhow::Result<bool, Self::Error> {
        let mut clients = self.ssh_clients.lock().await;
        clients.insert((self.client_id, channel.id()), session.handle());

        let mut channels = self.channels.lock().await;
        channels.insert(channel.id(), channel);

        Ok(true)
    }

    async fn channel_open_direct_tcpip(
        &mut self,
        channel: Channel<russh::server::Msg>,
        _host_to_connect: &str,
        port_to_connect: u32,
        _originator_address: &str,
        _originator_port: u32,
        _session: &mut Session,
    ) -> anyhow::Result<bool, Self::Error> {
        let grpc_port = self.server_response.port;
        if port_to_connect as u16 == grpc_port {
            let mut channel_stream = channel.into_stream();
            match tokio::net::TcpStream::connect(format!("127.0.0.1:{}", grpc_port)).await {
                Ok(mut stream) => {
                    tokio::spawn(async move {
                        match tokio::io::copy_bidirectional(&mut stream, &mut channel_stream).await
                        {
                            Ok((to_server, to_client)) => {
                                println!(
                                    "Port forwarding: {} bytes to server, {} bytes to client",
                                    to_server, to_client
                                );
                            }
                            Err(e) => {
                                eprintln!("Port forwarding error: {}", e);
                            }
                        }
                    });
                    Ok(true)
                }
                Err(e) => {
                    eprintln!("Failed to connect to GRPC server: {}", e);
                    Ok(false)
                }
            }
        } else {
            Ok(false)
        }
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> anyhow::Result<(), Self::Error> {
        let cmd = std::str::from_utf8(data).unwrap_or("");
        if cmd.contains("serve") {
            let response = ServeResult::Success(self.server_response.clone());
            let json = serde_json::to_string(&response)?;
            session.data(channel, json.into())?;
            session.eof(channel)?;
            session.close(channel)?;
        } else {
            session.data(channel, "Unsupported command".into())?;
            session.eof(channel)?;
            session.close(channel)?;
        }
        Ok(())
    }

    async fn subsystem_request(
        &mut self,
        channel_id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> anyhow::Result<(), Self::Error> {
        if name == "sftp" {
            let mut channels = self.channels.lock().await;
            if let Some(channel) = channels.remove(&channel_id) {
                // Create a channel stream
                let channel_stream = channel.into_stream();

                // Create SFTP session with repository path
                let sftp_session = SftpSession {
                    version: None,
                    repo_path: self.repo_path.clone(),
                    read_dirs: Default::default(),
                };

                // Signal success before starting the SFTP handler
                session.channel_success(channel_id)?;

                // Spawn SFTP handler task
                russh_sftp::server::run(channel_stream, sftp_session).await;

                return Ok(());
            }

            session.channel_failure(channel_id)?;
        } else {
            session.channel_failure(channel_id)?;
        }

        Ok(())
    }
}

// Define the SFTP session structure
#[derive(Default)]
struct SftpSession {
    version: Option<u32>,
    repo_path: PathBuf,
    read_dirs: HashMap<String, Option<Vec<File>>>,
}

#[async_trait::async_trait]
impl russh_sftp::server::Handler for SftpSession {
    type Error = StatusCode;

    fn unimplemented(&self) -> Self::Error {
        StatusCode::OpUnsupported
    }

    async fn init(
        &mut self,
        version: u32,
        _extensions: HashMap<String, String>,
    ) -> anyhow::Result<Version, Self::Error> {
        if self.version.is_some() {
            eprintln!("duplicate SSH_FXP_VERSION packet");
            return Err(StatusCode::ConnectionLost);
        }

        self.version = Some(version);
        Ok(Version::new())
    }

    async fn open(
        &mut self,
        id: u32,
        path: String,
        pflags: OpenFlags,
        _attrs: FileAttributes,
    ) -> anyhow::Result<Handle, Self::Error> {
        let file_path = self.repo_path.join(path.clone());
        // If the client wants to write/create the file, then do so.
        if pflags.contains(OpenFlags::CREATE) || pflags.contains(OpenFlags::WRITE) {
            // Open for writing (create if it doesn't exist)
            fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&file_path)
                .await
                .map_err(|_| StatusCode::PermissionDenied)?;
        } else {
            // Otherwise, we expect the file to exist
            if !file_path.exists() {
                return Err(StatusCode::NoSuchFile);
            }
        }
        Ok(Handle { id, handle: path })
    }

    async fn close(&mut self, id: u32, _handle: String) -> anyhow::Result<Status, Self::Error> {
        Ok(Status {
            id,
            status_code: StatusCode::Ok,
            error_message: "Ok".to_string(),
            language_tag: "en-US".to_string(),
        })
    }

    async fn read(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        len: u32,
    ) -> anyhow::Result<Data, Self::Error> {
        let file_path = self.repo_path.join(handle.clone());

        if !file_path.exists() || !file_path.is_file() {
            return Err(StatusCode::NoSuchFile);
        }

        let mut file = match fs::File::open(&file_path).await {
            Ok(f) => f,
            Err(_) => return Err(StatusCode::PermissionDenied),
        };

        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        if file.seek(std::io::SeekFrom::Start(offset)).await.is_err() {
            return Err(StatusCode::Failure);
        }

        let mut buffer = vec![0; len as usize];
        let bytes_read = match file.read(&mut buffer).await {
            Ok(n) => n,
            Err(_) => return Err(StatusCode::Failure),
        };

        buffer.truncate(bytes_read);

        if bytes_read == 0 {
            return Err(StatusCode::Eof);
        }

        Ok(Data { id, data: buffer })
    }

    async fn write(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        data: Vec<u8>,
    ) -> anyhow::Result<Status, Self::Error> {
        let file_path = self.repo_path.join(handle.clone());
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&file_path)
            .await
            .map_err(|_| StatusCode::PermissionDenied)?;
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|_| StatusCode::Failure)?;
        file.write_all(&data)
            .await
            .map_err(|_| StatusCode::Failure)?;
        Ok(Status {
            id,
            status_code: StatusCode::Ok,
            error_message: "Ok".to_string(),
            language_tag: "en-US".to_string(),
        })
    }

    async fn setstat(
        &mut self,
        id: u32,
        path: String,
        _attrs: FileAttributes,
    ) -> anyhow::Result<Status, Self::Error> {
        // Build the full filesystem path.
        let file_path = self.repo_path.join(path);
        // Check that the file or directory exists.
        if file_path.exists() {
            // (Optionally update times/attributes here, for example using the `filetime` crate.)
            // For now, simply acknowledge the request.
            Ok(Status {
                id,
                status_code: StatusCode::Ok,
                error_message: "Ok".to_string(),
                language_tag: "en-US".to_string(),
            })
        } else {
            // If the file doesn't exist, return an error.
            Err(StatusCode::NoSuchFile)
        }
    }

    async fn opendir(&mut self, id: u32, path: String) -> anyhow::Result<Handle, Self::Error> {
        let dir_path = self.repo_path.join(path.clone());

        if !dir_path.exists() || !dir_path.is_dir() {
            return Err(StatusCode::NoSuchFile);
        }

        // Pre-read directory entries and store them.
        let mut read_dir = tokio::fs::read_dir(&dir_path)
            .await
            .map_err(|_| StatusCode::PermissionDenied)?;
        let mut files = Vec::new();

        while let Ok(Some(entry)) = read_dir.next_entry().await {
            let file_name = entry.file_name().to_string_lossy().to_string();
            let file_type = match entry.file_type().await {
                Ok(ft) => ft,
                Err(_) => continue,
            };

            let metadata = match entry.metadata().await {
                Ok(meta) => meta,
                Err(_) => continue,
            };

            let attrs = FileAttributes {
                size: Some(metadata.len()),
                uid: Some(0),
                user: None,
                gid: Some(0),
                group: None,
                permissions: Some(
                    metadata.permissions().mode() | if file_type.is_dir() { 0x4000 } else { 0 },
                ),
                atime: Some(
                    metadata
                        .accessed()
                        .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as u32,
                ),
                mtime: Some(
                    metadata
                        .modified()
                        .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as u32,
                ),
            };
            files.push(File::new(file_name, attrs));
        }

        // Save the list under the directory handle.
        self.read_dirs.insert(path.clone(), Some(files));

        Ok(Handle { id, handle: path })
    }

    async fn readdir(&mut self, id: u32, handle: String) -> anyhow::Result<Name, Self::Error> {
        if let Some(dir_entry_opt) = self.read_dirs.get_mut(&handle) {
            return if let Some(files) = dir_entry_opt.take() {
                Ok(Name { id, files })
            } else {
                // Entries already returned – signal EOF.
                Err(StatusCode::Eof)
            };
        }
        Err(StatusCode::NoSuchFile)
    }

    async fn mkdir(
        &mut self,
        id: u32,
        path: String,
        _attrs: FileAttributes,
    ) -> anyhow::Result<Status, Self::Error> {
        let dir_path = self.repo_path.join(path);
        match tokio::fs::create_dir(&dir_path).await {
            Ok(()) => Ok(Status {
                id,
                status_code: StatusCode::Ok,
                error_message: "Ok".to_string(),
                language_tag: "en-US".to_string(),
            }),
            Err(e) => {
                eprintln!("mkdir error: {}", e);
                Err(StatusCode::Failure)
            }
        }
    }

    async fn realpath(&mut self, id: u32, path: String) -> anyhow::Result<Name, Self::Error> {
        let clean_path = if path.is_empty() || path == "." {
            "/".to_string()
        } else {
            path
        };

        let file_path = self.repo_path.join(clean_path.clone());

        if !file_path.exists() {
            return Err(StatusCode::NoSuchFile);
        }

        let attrs = if let Ok(metadata) = tokio::fs::metadata(&file_path).await {
            FileAttributes {
                size: Some(metadata.len()),
                uid: Some(0),
                user: None,
                gid: Some(0),
                group: None,
                permissions: Some(metadata.permissions().mode()),
                atime: Some(
                    metadata
                        .accessed()
                        .unwrap()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as u32,
                ),
                mtime: Some(
                    metadata
                        .modified()
                        .unwrap()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as u32,
                ),
            }
        } else {
            FileAttributes::default()
        };

        Ok(Name {
            id,
            files: vec![File::new(clean_path, attrs)],
        })
    }

    async fn stat(&mut self, id: u32, path: String) -> anyhow::Result<Attrs, Self::Error> {
        let file_path = self.repo_path.join(path);

        if !file_path.exists() {
            return Err(StatusCode::NoSuchFile);
        }

        let metadata = match tokio::fs::metadata(&file_path).await {
            Ok(meta) => meta,
            Err(_) => return Err(StatusCode::PermissionDenied),
        };

        let attrs = FileAttributes {
            size: Some(metadata.len()),
            uid: Some(0),
            user: None,
            gid: Some(0),
            group: None,
            permissions: Some(metadata.permissions().mode()),
            atime: Some(
                metadata
                    .accessed()
                    .unwrap()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as u32,
            ),
            mtime: Some(
                metadata
                    .modified()
                    .unwrap()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as u32,
            ),
        };

        Ok(Attrs { id, attrs })
    }

    async fn rename(
        &mut self,
        id: u32,
        oldpath: String,
        newpath: String,
    ) -> anyhow::Result<Status, Self::Error> {
        let old_full = self.repo_path.join(oldpath);
        let new_full = self.repo_path.join(newpath);

        // If the destination exists, try removing it first.
        if new_full.exists() {
            if let Err(e) = tokio::fs::remove_file(&new_full).await {
                eprintln!(
                    "rename: failed to remove existing {}: {}",
                    new_full.display(),
                    e
                );
                // If removal fails, report failure.
                return Err(StatusCode::Failure);
            }
        }

        match tokio::fs::rename(&old_full, &new_full).await {
            Ok(()) => Ok(Status {
                id,
                status_code: StatusCode::Ok,
                error_message: "Ok".to_string(),
                language_tag: "en-US".to_string(),
            }),
            Err(e) => {
                eprintln!("rename error: {}", e);
                Err(StatusCode::Failure)
            }
        }
    }
}

pub async fn start_ssh_server(
    repo_path: &Path,
    ssh_port: u16,
    password: String,
) -> anyhow::Result<Pin<Box<dyn Future<Output = ()>>>, anyhow::Error> {
    let (tx, rx) = oneshot::channel();

    let key_pair = PrivateKey::random(&mut OsRng, Algorithm::Ed25519)?;

    let config = russh::server::Config {
        auth_rejection_time: std::time::Duration::from_secs(1),
        auth_rejection_time_initial: Some(std::time::Duration::from_secs(0)),
        keys: vec![key_pair],
        ..Default::default()
    };

    let auth_key = serve::generate_auth_key();
    let port = find_available_port().await?;

    let auth_key_clone = auth_key.clone();
    let server_response = ServeResponse { port, auth_key };
    let repo_path_clone = repo_path.to_path_buf();
    let server_handle = tokio::spawn(async move {
        let (tx, _rx): (UnboundedSender<Vec<u8>>, UnboundedReceiver<Vec<u8>>) = unbounded_channel();
        let writer = ChannelWriter::new(tx);
        let writer: Box<dyn Write + Send + Sync> = Box::new(writer);
        let writer = std::sync::Arc::new(std::sync::Mutex::new(writer));

        if let Err(e) = serve::serve_on_port(
            Some(repo_path_clone),
            Output::Override(writer),
            port,
            auth_key_clone,
            async {
                let _ = rx.await;
            },
        )
        .await
        {
            eprintln!("GRPC server error: {}", e);
        }
        println!("GRPC server on port {port} stopped");
    });

    let server = SshServer {
        password: password.clone(),
        repo_path: repo_path.to_path_buf(),
        ssh_clients: std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        client_id: 0,
        server_response,
    };

    let addr = std::net::SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        ssh_port,
    );
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    tokio::spawn(async move {
        let config = std::sync::Arc::new(config);

        loop {
            match listener.accept().await {
                Ok((socket, _)) => {
                    let mut server = server.clone();
                    let config = config.clone();

                    tokio::spawn(async move {
                        let handler = server.new_client(socket.peer_addr().ok());
                        if let Err(e) = russh::server::run_stream(config, socket, handler).await {
                            eprintln!("SSH connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("SSH server accept error: {}", e);
                    break;
                }
            }
        }
    });

    Ok(Box::pin(async {
        tx.send(()).expect("channel should not be closed");
        let _ = server_handle.await;
    }))
}
