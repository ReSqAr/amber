use amber::cli::{Cli, run_cli};
use amber::commands::serve;
use amber::flightdeck::output::Output;
use anyhow::anyhow;
use cipher::crypto_common::rand_core::OsRng;
use clap::Parser;
use russh::keys::{Algorithm, PrivateKey};
use russh::server::{Auth, Handler, Server, Session};
use russh::{Channel, ChannelId};
use russh_sftp::protocol::{
    Attrs, Data, File, FileAttributes, Handle, Name, OpenFlags, Status, StatusCode, Version,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use tempfile::tempdir;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Refined DSL command enum.
#[derive(Debug)]
enum CommandLine {
    AmberCommand {
        repo: String,
        sub_command: Vec<String>,
    },
    RandomFile {
        repo: String,
        filename: String,
        size: usize,
    },
    WriteFile {
        repo: String,
        filename: String,
        data: String,
    },
    RemoveFile {
        repo: String,
        filename: String,
    },
    AssertExists {
        repo: String,
        filename: String,
        content: Option<String>,
    },
    AssertDoesNotExist {
        repo: String,
        filename: String,
    },
    AssertEqual {
        left_repo: String,
        right_repo: String,
    },
    AssertHardlinked {
        repo: String,
        filename1: String,
        filename2: String,
    },
    AssertOutputContains {
        expected: String,
    },
    StartSsh {
        repo: String,
        port: u16,
        password: String,
    },
    EndSsh {
        repo: String,
    },
}

/// A repository instance in our DSL environment.
#[derive(Debug)]
struct RepoInstance {
    #[allow(dead_code)]
    id: String,
    path: PathBuf,
}

/// The test environment holds a temporary $ROOT directory and a map of repository instances.
#[derive(Debug)]
struct TestEnv {
    #[allow(dead_code)]
    root: PathBuf,
    repos: HashMap<String, RepoInstance>,
}

/// Tokenize a DSL line while respecting single/double quotes and stripping trailing comments.
fn tokenize_line(line: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    for c in line.chars() {
        // Unquoted '#' starts a comment.
        if c == '#' && !in_single_quote && !in_double_quote {
            break;
        }
        if c.is_whitespace() && !in_single_quote && !in_double_quote {
            if !token.is_empty() {
                tokens.push(token.clone());
                token.clear();
            }
        } else if c == '\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        } else if c == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        } else {
            token.push(c);
        }
    }
    if !token.is_empty() {
        tokens.push(token);
    }
    tokens
}

/// Parse a DSL line into one of the refined CommandLine variants.
fn parse_line(line: &str) -> Option<CommandLine> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    let tokens = tokenize_line(trimmed);
    if tokens.is_empty() {
        return None;
    }
    match tokens[0].as_str() {
        "assert_equal" => {
            if tokens.len() != 3 {
                panic!("Invalid assert_equal command: {}", line);
            }
            Some(CommandLine::AssertEqual {
                left_repo: tokens[1].to_string(),
                right_repo: tokens[2].to_string(),
            })
        }
        // <-- New DSL command parsing branch added here:
        "assert_output_contains" => {
            if tokens.len() != 2 {
                panic!("Invalid assert_output_contains command: {}", line);
            }
            Some(CommandLine::AssertOutputContains {
                expected: tokens[1].to_string(),
            })
        }
        token if token.starts_with('@') => {
            let repo = token[1..].to_string();
            if tokens.len() < 2 {
                panic!("No command specified for repo {} in line: {}", repo, line);
            }
            let command = tokens[1].as_str();
            match command {
                "amber" => {
                    // All tokens from index 1 onward form the sub-command.
                    let sub_command = tokens[1..].iter().map(|s| s.to_string()).collect();
                    Some(CommandLine::AmberCommand { repo, sub_command })
                }
                "random_file" => {
                    if tokens.len() != 4 {
                        panic!("Invalid random_file command: {}", line);
                    }
                    let filename = tokens[2].to_string();
                    let size: usize = tokens[3]
                        .parse()
                        .expect("Invalid size in random_file command");
                    Some(CommandLine::RandomFile {
                        repo,
                        filename,
                        size,
                    })
                }
                "write_file" => {
                    if tokens.len() != 4 {
                        panic!("Invalid write_file command: {}", line);
                    }
                    let filename = tokens[2].to_string();
                    let data = tokens[3].to_string();
                    Some(CommandLine::WriteFile {
                        repo,
                        filename,
                        data,
                    })
                }
                "remove_file" => {
                    if tokens.len() != 3 {
                        panic!("Invalid remove command: {}", line);
                    }
                    let filename = tokens[2].to_string();
                    Some(CommandLine::RemoveFile { repo, filename })
                }
                "assert_exists" => {
                    // Can be either: @repo assert_exists filename
                    // or: @repo assert_exists filename "expected content"
                    if tokens.len() < 3 || tokens.len() > 4 {
                        panic!("Invalid assert_exists command: {}", line);
                    }
                    let filename = tokens[2].to_string();
                    let content = if tokens.len() == 4 {
                        Some(tokens[3].to_string())
                    } else {
                        None
                    };
                    Some(CommandLine::AssertExists {
                        repo,
                        filename,
                        content,
                    })
                }
                "assert_does_not_exist" => {
                    if tokens.len() != 3 {
                        panic!("Invalid assert_does_not_exist command: {}", line);
                    }
                    let filename = tokens[2].to_string();
                    Some(CommandLine::AssertDoesNotExist { repo, filename })
                }
                "assert_hardlinked" => {
                    if tokens.len() != 4 {
                        panic!("Invalid assert_hardlinked command: {}", line);
                    }
                    let filename1 = tokens[2].to_string();
                    let filename2 = tokens[3].to_string();
                    Some(CommandLine::AssertHardlinked {
                        repo,
                        filename1,
                        filename2,
                    })
                }
                "start_ssh" => {
                    if tokens.len() != 4 {
                        panic!("Invalid start_ssh command: {}", line);
                    }
                    let port = tokens[2]
                        .parse::<u16>()
                        .expect("Invalid port in start_ssh command");
                    let password = tokens[3].to_string();
                    Some(CommandLine::StartSsh {
                        repo,
                        port,
                        password,
                    })
                }
                "end_ssh" => {
                    if tokens.len() != 2 {
                        panic!("Invalid end_ssh command: {}", line);
                    }
                    Some(CommandLine::EndSsh { repo })
                }
                other => panic!("Unknown repository command '{}' in line: {}", other, line),
            }
        }
        _ => panic!("Unrecognized DSL command: {}", line),
    }
}

// escape hatch of all escape hatches
pub struct ChannelWriter {
    sender: UnboundedSender<Vec<u8>>,
}

impl ChannelWriter {
    pub fn new(sender: UnboundedSender<Vec<u8>>) -> Self {
        Self { sender }
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let data = buf.to_vec();
        self.sender
            .send(data)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Channel closed"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// run_cli_command now takes an additional `root` parameter and replaces all occurrences of "$ROOT"
/// in the arguments.
async fn run_cli_command(
    args: &[String],
    working_dir: &Path,
    root: &Path,
) -> anyhow::Result<String, anyhow::Error> {
    let substituted: Vec<String> = args
        .iter()
        .map(|arg| arg.replace("$ROOT", &root.to_string_lossy()))
        .collect();

    let mut cli = Cli::try_parse_from(&substituted)?;
    cli.path = Some(working_dir.to_path_buf());

    let (tx, rx): (UnboundedSender<Vec<u8>>, UnboundedReceiver<Vec<u8>>) = unbounded_channel();
    let writer = ChannelWriter::new(tx);
    let writer = std::sync::Arc::new(std::sync::Mutex::new(
        Box::new(writer) as Box<dyn Write + Send + Sync>
    ));

    let current_dur = env::current_dir()?;
    env::set_current_dir(working_dir)?;
    run_cli(cli, Output::Override(writer)).await?;
    env::set_current_dir(current_dur)?;

    let chunks: Vec<Vec<u8>> = UnboundedReceiverStream::new(rx).collect().await;
    let combined: Vec<u8> = chunks.into_iter().flatten().collect();
    let output: String = String::from_utf8_lossy(&combined).into();
    println!(
        "{}",
        output
            .lines()
            .map(|l| format!("     > {}", l))
            .collect::<Vec<String>>()
            .join("\n")
    );

    Ok(output)
}

/// Helper to create a random file with random content.
async fn create_random_file(
    dir: &Path,
    filename: &str,
    size: usize,
) -> anyhow::Result<(), anyhow::Error> {
    let file_path = dir.join(filename);
    let mut file = fs::File::create(&file_path).await?;
    let content: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
    file.write_all(&content).await?;
    Ok(())
}

/// Helper to write a file with specific content.
async fn write_file(
    dir: &Path,
    filename: &str,
    content: &str,
) -> anyhow::Result<(), anyhow::Error> {
    let file_path = dir.join(filename);
    fs::write(&file_path, content).await?;
    Ok(())
}

/// Helper to remove a file.
async fn remove_file(dir: &Path, filename: &str) -> anyhow::Result<(), anyhow::Error> {
    let file_path = dir.join(filename);
    fs::remove_file(&file_path).await?;
    Ok(())
}

/// Helper to assert that a file exists. If `expected_content` is provided, its content is compared.
async fn assert_file_exists(
    dir: &Path,
    filename: &str,
    expected_content: &Option<String>,
) -> anyhow::Result<(), anyhow::Error> {
    let file_path = dir.join(filename);
    if !file_path.exists() {
        return Err(anyhow!("File {} does not exist", filename));
    }
    if let Some(expected) = expected_content {
        let content = fs::read_to_string(&file_path).await?;
        if content != *expected {
            return Err(anyhow!(
                "File {} content mismatch: expected '{}', got '{}'",
                filename,
                expected,
                content
            ));
        }
    }
    Ok(())
}

/// Helper to assert that a file does not exist.
async fn assert_file_does_not_exist(
    dir: &Path,
    filename: &str,
) -> anyhow::Result<(), anyhow::Error> {
    let file_path = dir.join(filename);
    if file_path.exists() {
        return Err(anyhow!("File {} exists, but it should not", filename));
    }
    Ok(())
}

/// Helper to assert that two files in the same directory are hardlinked.
/// On Unix-like systems this compares the inode numbers.
async fn assert_files_hardlinked(
    dir: &Path,
    file1: &str,
    file2: &str,
) -> anyhow::Result<(), anyhow::Error> {
    let path1 = dir.join(file1);
    let path2 = dir.join(file2);

    let meta1 = fs::metadata(&path1).await?;
    let meta2 = fs::metadata(&path2).await?;

    {
        use anyhow::anyhow;
        use std::os::unix::fs::MetadataExt;
        if meta1.ino() != meta2.ino() {
            return Err(anyhow!("Files {} and {} are not hardlinked", file1, file2));
        }
    }

    Ok(())
}

/// Recursively compare non–hidden files in two directories.
async fn assert_directories_equal(dir1: &Path, dir2: &Path) -> anyhow::Result<(), anyhow::Error> {
    let mut entries1 = fs::read_dir(dir1).await?;
    let mut files1 = Vec::new();
    while let Some(entry) = entries1.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') {
            continue;
        }
        files1.push(name);
    }
    let mut entries2 = fs::read_dir(dir2).await?;
    let mut files2 = Vec::new();
    while let Some(entry) = entries2.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') {
            continue;
        }
        files2.push(name);
    }
    files1.sort();
    files2.sort();
    if files1 != files2 {
        return Err(anyhow!(
            "directory file lists differ: left={:?} vs right={:?}",
            files1,
            files2
        ));
    }
    for file in files1 {
        let path1 = dir1.join(&file);
        let path2 = dir2.join(&file);
        let content1 = fs::read(&path1).await?;
        let content2 = fs::read(&path2).await?;
        if content1 != content2 {
            return Err(anyhow!("file content {} differs between directories", file));
        }
    }
    Ok(())
}

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

pub(crate) async fn find_available_port() -> std::result::Result<u16, anyhow::Error> {
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
            tokio::fs::OpenOptions::new()
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

        let mut file = match tokio::fs::File::open(&file_path).await {
            Ok(f) => f,
            Err(_) => return Err(StatusCode::PermissionDenied),
        };

        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        if (file.seek(std::io::SeekFrom::Start(offset)).await).is_err() {
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
        let mut file = tokio::fs::OpenOptions::new()
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

async fn start_ssh_server(
    repo_path: &Path,
    ssh_port: u16,
    password: String,
) -> anyhow::Result<Box<dyn FnOnce()>, anyhow::Error> {
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
    let _server_handle = tokio::spawn(async move {
        let (tx, _rx): (UnboundedSender<Vec<u8>>, UnboundedReceiver<Vec<u8>>) = unbounded_channel();
        let writer = ChannelWriter::new(tx);
        let writer = std::sync::Arc::new(std::sync::Mutex::new(
            Box::new(writer) as Box<dyn Write + Send + Sync>
        ));

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
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

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

    Ok(Box::new(|| tx.send(()).expect("reason")))
}

/// Run the DSL script. This function creates a temporary $ROOT directory,
/// then processes each DSL line.
pub async fn run_dsl_script(script: &str) -> anyhow::Result<(), anyhow::Error> {
    // Create a temporary root directory.
    let tmp_dir = tempdir()?;
    let root = tmp_dir.path().to_path_buf();
    let mut last_command_output: String = "".into();

    let mut env = TestEnv {
        root: root.clone(),
        repos: HashMap::new(),
    };

    let mut ssh_connection = HashMap::new();

    let mut line_number = 0usize;
    for line in script.lines() {
        if !ssh_connection.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        line_number += 1;
        println!("[{:2}] {}", line_number, line.trim());
        if let Some(cmd) = parse_line(line) {
            match cmd {
                CommandLine::AmberCommand { repo, sub_command } => {
                    let repo_instance = env.repos.entry(repo.clone()).or_insert_with(|| {
                        let repo_path = root.join(&repo);
                        std::fs::create_dir_all(&repo_path)
                            .expect("failed to create repository folder");
                        RepoInstance {
                            id: repo.clone(),
                            path: repo_path,
                        }
                    });
                    // Call run_cli_command, passing in the global root for $ROOT substitution.
                    last_command_output =
                        run_cli_command(&sub_command, &repo_instance.path, &root).await?;
                }
                CommandLine::RandomFile {
                    repo,
                    filename,
                    size,
                } => {
                    let repo_instance = env.repos.entry(repo.clone()).or_insert_with(|| {
                        let repo_path = root.join(&repo);
                        std::fs::create_dir_all(&repo_path)
                            .expect("failed to create repository folder");
                        RepoInstance {
                            id: repo.clone(),
                            path: repo_path,
                        }
                    });
                    create_random_file(&repo_instance.path, &filename, size).await?;
                }
                CommandLine::WriteFile {
                    repo,
                    filename,
                    data,
                } => {
                    let repo_instance = env.repos.entry(repo.clone()).or_insert_with(|| {
                        let repo_path = root.join(&repo);
                        std::fs::create_dir_all(&repo_path)
                            .expect("failed to create repository folder");
                        RepoInstance {
                            id: repo.clone(),
                            path: repo_path,
                        }
                    });
                    write_file(&repo_instance.path, &filename, &data).await?;
                }
                CommandLine::RemoveFile { repo, filename } => {
                    let repo_instance = env.repos.get(&repo).ok_or_else(|| {
                        anyhow!("Repository {} not found for remove command", repo)
                    })?;
                    remove_file(&repo_instance.path, &filename).await?;
                }
                CommandLine::AssertExists {
                    repo,
                    filename,
                    content,
                } => {
                    let repo_instance = env.repos.get(&repo).ok_or_else(|| {
                        anyhow!("Repository {} not found for assert_exists command", repo)
                    })?;
                    assert_file_exists(&repo_instance.path, &filename, &content).await?;
                }
                CommandLine::AssertDoesNotExist { repo, filename } => {
                    let repo_instance = env.repos.get(&repo).ok_or_else(|| {
                        anyhow!(
                            "repository {} not found for assert_does_not_exist command",
                            repo
                        )
                    })?;
                    assert_file_does_not_exist(&repo_instance.path, &filename).await?;
                }
                CommandLine::AssertEqual {
                    left_repo,
                    right_repo,
                } => {
                    let left = env.repos.get(&left_repo).ok_or_else(|| {
                        anyhow!("repository {} not found for assert_equal", left_repo)
                    })?;
                    let right = env.repos.get(&right_repo).ok_or_else(|| {
                        anyhow!("repository {} not found for assert_equal", right_repo)
                    })?;
                    assert_directories_equal(&left.path, &right.path)
                        .await
                        .map_err(|e| anyhow!("assert_equal failed: {}", e))?;
                }
                CommandLine::AssertHardlinked {
                    repo,
                    filename1,
                    filename2,
                } => {
                    let repo_instance = env.repos.get(&repo).ok_or_else(|| {
                        anyhow!(
                            "repository {} not found for assert_hardlinked command",
                            repo
                        )
                    })?;
                    assert_files_hardlinked(&repo_instance.path, &filename1, &filename2).await?;
                }
                CommandLine::AssertOutputContains { expected } => {
                    if !last_command_output.contains(&expected) {
                        return Err(anyhow!(
                            "assert_output_contains failed: output did not contain '{}'",
                            expected
                        ));
                    }
                }
                CommandLine::StartSsh {
                    repo,
                    port,
                    password,
                } => {
                    let repo_instance = env.repos.get(&repo).ok_or_else(|| {
                        anyhow!("Repository {} not found for start_ssh command", repo)
                    })?;

                    let shutdown = start_ssh_server(&repo_instance.path, port, password).await?;
                    if let Some(s) = ssh_connection.insert(repo, shutdown) {
                        s()
                    }
                }
                CommandLine::EndSsh { repo } => {
                    if let Some(s) = ssh_connection.remove(&repo) {
                        s()
                    }
                }
            }
        }
    }
    Ok(())
}
