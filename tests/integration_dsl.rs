use amber::cli::Cli;
use amber::cli::run_cli;
use amber::commands::serve;
use amber::flightdeck::output::Output;
use anyhow::{Result, anyhow};
use clap::Parser;
use russh::keys::signature::rand_core::OsRng;
use russh::keys::{Algorithm, PrivateKey};
use russh::server::{Auth, Handler, Server, Session};
use russh::{Channel, ChannelId};
use serde::{Deserialize, Serialize};
use serial_test::serial;
use std::collections::HashMap;
use std::env;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::tempdir;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
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
) -> Result<String, anyhow::Error> {
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
async fn create_random_file(dir: &Path, filename: &str, size: usize) -> Result<(), anyhow::Error> {
    let file_path = dir.join(filename);
    let mut file = fs::File::create(&file_path).await?;
    let content: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
    file.write_all(&content).await?;
    Ok(())
}

/// Helper to write a file with specific content.
async fn write_file(dir: &Path, filename: &str, content: &str) -> Result<(), anyhow::Error> {
    let file_path = dir.join(filename);
    fs::write(&file_path, content).await?;
    Ok(())
}

/// Helper to remove a file.
async fn remove_file(dir: &Path, filename: &str) -> Result<(), anyhow::Error> {
    let file_path = dir.join(filename);
    fs::remove_file(&file_path).await?;
    Ok(())
}

/// Helper to assert that a file exists. If `expected_content` is provided, its content is compared.
async fn assert_file_exists(
    dir: &Path,
    filename: &str,
    expected_content: &Option<String>,
) -> Result<(), anyhow::Error> {
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
async fn assert_file_does_not_exist(dir: &Path, filename: &str) -> Result<(), anyhow::Error> {
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
) -> Result<(), anyhow::Error> {
    let path1 = dir.join(file1);
    let path2 = dir.join(file2);

    let meta1 = fs::metadata(&path1).await?;
    let meta2 = fs::metadata(&path2).await?;

    {
        use std::os::unix::fs::MetadataExt;
        if meta1.ino() != meta2.ino() {
            return Err(anyhow!("Files {} and {} are not hardlinked", file1, file2));
        }
    }

    Ok(())
}

/// Recursively compare non–hidden files in two directories.
async fn assert_directories_equal(dir1: &Path, dir2: &Path) -> Result<(), anyhow::Error> {
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
    server_response: std::sync::Arc<tokio::sync::Mutex<Option<ServeResponse>>>,
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
        }
    }
}

struct SshSession {
    client_id: usize,
    password: String,
    repo_path: PathBuf,
    ssh_clients:
        std::sync::Arc<tokio::sync::Mutex<HashMap<(usize, ChannelId), russh::server::Handle>>>,
    server_response: std::sync::Arc<tokio::sync::Mutex<Option<ServeResponse>>>,
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

    async fn auth_password(&mut self, _user: &str, password: &str) -> Result<Auth, Self::Error> {
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
    ) -> Result<bool, Self::Error> {
        let mut clients = self.ssh_clients.lock().await;
        clients.insert((self.client_id, channel.id()), session.handle());

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
    ) -> Result<bool, Self::Error> {
        if let Some(ServeResponse {
            port: grpc_port, ..
        }) = &self.server_response.lock().await.clone()
        {
            if port_to_connect as u16 == *grpc_port {
                let mut channel_stream = channel.into_stream();

                return match tokio::net::TcpStream::connect(format!("127.0.0.1:{}", grpc_port))
                    .await
                {
                    Ok(mut stream) => {
                        tokio::spawn(async move {
                            match tokio::io::copy_bidirectional(&mut stream, &mut channel_stream)
                                .await
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
                };
            }
        }

        Ok(false)
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let cmd = std::str::from_utf8(data).unwrap_or("");

        if cmd.contains("serve") {
            let server_response =
                if let Some(server_response) = self.server_response.lock().await.clone() {
                    server_response
                } else {
                    let auth_key = serve::generate_auth_key();

                    let port = match find_available_port().await {
                        Ok(p) => p,
                        Err(e) => {
                            let error_result = ServeResult::Error(ServeError {
                                error: format!("Failed to find available port: {}", e),
                            });
                            let json = serde_json::to_string(&error_result)?;

                            session.data(channel, json.into())?;
                            session.eof(channel)?;
                            session.close(channel)?;
                            return Ok(());
                        }
                    };

                    let auth_key_clone = auth_key.clone();
                    let server_response = ServeResponse { port, auth_key };
                    *self.server_response.lock().await = Some(server_response.clone());

                    let repo_path_clone = self.repo_path.clone();
                    let _server_handle = tokio::spawn(async move {
                        let (tx, _rx): (UnboundedSender<Vec<u8>>, UnboundedReceiver<Vec<u8>>) =
                            unbounded_channel();
                        let writer = ChannelWriter::new(tx);
                        let writer = std::sync::Arc::new(std::sync::Mutex::new(
                            Box::new(writer) as Box<dyn Write + Send + Sync>
                        ));

                        if let Err(e) = serve::serve_on_port(
                            Some(repo_path_clone),
                            Output::Override(writer),
                            port,
                            auth_key_clone,
                            tokio::time::sleep(std::time::Duration::from_secs(3600)), // TODO: better cleanup?
                        )
                        .await
                        {
                            eprintln!("GRPC server error: {}", e);
                        }
                    });
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                    server_response
                };

            let response = ServeResult::Success(server_response);
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
}

async fn start_ssh_server(
    repo_path: &Path,
    ssh_port: u16,
    password: String,
) -> Result<(), anyhow::Error> {
    println!("Starting ssh server");
    let key_pair = PrivateKey::random(&mut OsRng, Algorithm::Ed25519)?;

    let config = russh::server::Config {
        auth_rejection_time: std::time::Duration::from_secs(1),
        auth_rejection_time_initial: Some(std::time::Duration::from_secs(0)),
        keys: vec![key_pair],
        ..Default::default()
    };

    let server = SshServer {
        password: password.clone(),
        repo_path: repo_path.to_path_buf(),
        ssh_clients: std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        client_id: 0,
        server_response: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
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

    // Give the server time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    Ok(())
}

/// Run the DSL script. This function creates a temporary $ROOT directory,
/// then processes each DSL line.
async fn run_dsl_script(script: &str) -> Result<(), anyhow::Error> {
    // Create a temporary root directory.
    let tmp_dir = tempdir()?;
    let root = tmp_dir.path().to_path_buf();
    let mut last_command_output: String = "".into();

    let mut env = TestEnv {
        root: root.clone(),
        repos: HashMap::new(),
    };

    let mut line_number = 0usize;
    for line in script.lines() {
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

                    start_ssh_server(&repo_instance.path, port, password).await?;
                }
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_auto_restore_removed_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "This file will be restored"
        @a assert_exists test.txt "This file will be restored"
        @a amber add
        @a remove_file test.txt
        @a assert_does_not_exist test.txt

        # action
        @a amber sync

        # then
        @a assert_exists test.txt "This file will be restored"
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_deduplicate() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test-1.txt "I am not a snowflake"
        @a write_file test-2.txt "I am not a snowflake"

        # action
        @a amber add

        # then
        @a assert_hardlinked test-1.txt test-2.txt
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_sync() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a random_file test-a.txt 100
        @a amber add

        @b amber init b
        @b write_file test-b.txt "Hello world!"
        @b amber add

        @a amber remote add b local $ROOT/b

        # action
        @a amber push b
        @a amber pull b
        @b amber sync

        # then
        assert_equal a b
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rclone_repo() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello store!"
        @a amber add

        @b amber init b

        @a amber remote add b local $ROOT/b
        @a amber remote add store rclone :local:/$ROOT/rclone
        @b amber remote add store rclone :local:/$ROOT/rclone

        # action
        @a amber push store
        @a amber sync b
        @b amber pull store

        # then
        assert_equal a b
        @b assert_exists test.txt "Hello store!"
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_push_path_selector() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file to-be-copied.txt "content to be transfered" 
        @a write_file not-transfered.txt "content only in a" 
        @a amber add

        @b amber init b

        @a amber remote add b local $ROOT/b

        # action
        @a amber push b to-be-copied.txt
        @b amber sync

        # then
        @b assert_exists to-be-copied.txt "content to be transfered"
        @b assert_does_not_exist not-transfered.txt
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_pull_path_selector() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        @b amber init b
        @b write_file to-be-copied.txt "content to be transfered" 
        @b write_file not-transfered.txt "content only in a" 
        @b amber add

        @a amber remote add b local $ROOT/b

        # action
        @a amber pull b to-be-copied.txt

        # then
        @a assert_exists to-be-copied.txt "content to be transfered"
        @a assert_does_not_exist not-transfered.txt
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_sync_same_filename_pull_push() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @b amber init b
        @a write_file test.txt "Hello A world!"
        @a amber add
        @b write_file test.txt "Hello B world!"
        @b amber add
        @a amber remote add b local $ROOT/b

        # action
        @a amber pull b
        @a amber push b
        @b amber sync

        # then
        assert_equal a b
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_sync_same_filename_push_pull() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @b amber init b
        @a write_file test.txt "Hello world - I am A!"
        @a amber add
        @b write_file test.txt "Hello world - I am B!"
        @b amber add
        @a amber remote add b local $ROOT/b

        # action
        @a amber push b
        @a amber pull b
        @b amber sync

        # then
        @a assert_exists test.txt "Hello world - I am B!"
        @b assert_exists test.txt "Hello world - I am B!"
        assert_equal a b
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_missing() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @b amber init b
        @a write_file test-a.txt "Hello A world!"
        @a amber add
        @b write_file test-b.txt "Hello B world!"
        @b amber add
        @a amber remote add b local $ROOT/b

        # action 1
        @a amber sync b
        
        # then
        @a amber missing
        assert_output_contains "missing test-b.txt (exists in: b)"
        @b amber missing
        assert_output_contains "missing test-a.txt (exists in: a)"

        # action 2
        @a amber pull b

        # then
        @a amber missing
        assert_output_contains "no files missing"
        @b amber missing
        assert_output_contains "missing test-a.txt (exists in: a)"

        # action 3
        @a amber push b

        # then
        @a amber missing
        assert_output_contains "no files missing"
        @b amber missing
        assert_output_contains "no files missing"
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_status_missing() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @b amber init b
        @a write_file test-a.txt "Hello A world!"
        @a amber add
        @b write_file test-b.txt "Hello B world!"
        @b amber add
        @a amber remote add b local $ROOT/b

        # action 1
        @a amber sync b

        # then
        @a amber status
        assert_output_contains "missing test-b.txt"
        @b amber status
        assert_output_contains "missing test-a.txt"

        # action 2
        @a amber pull b

        # then
        @a amber status
        assert_output_contains "detected 2 materialised files"
        @b amber status
        assert_output_contains "missing test-a.txt"

        # action 3
        @a amber push b
        @b amber sync

        # then
        @a amber status
        assert_output_contains "detected 2 materialised files"
        @b amber status
        assert_output_contains "detected 2 materialised files"
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_altered_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Original content"
        @a amber add
        # Simulate user alteration by overwriting the file with wrong content.
        @a remove_file test.txt
        @a write_file test.txt "User altered content"

        @a amber sync

        # action
        @a amber status

        # then: we do not overwrite the on-disk file but do alert the user
        assert_output_contains "altered test.txt"
        @a assert_exists test.txt "User altered content"
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_outdated_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello A world!"
        @a amber add

        @b amber init b
        @b write_file test.txt "Hello B world!"
        @b amber add

        @a amber remote add b local $ROOT/b
        @a amber sync b

        # action
        @a amber status
        # then
        assert_output_contains "outdated test.txt"

        # action
        @a amber pull b
        @a amber push b
        @b amber sync
        # then
        assert_equal a b
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_missing_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        @b amber init b
        @b write_file test.txt "Hello B world!"
        @b amber add

        @a amber remote add b local $ROOT/b
        @a amber sync b

        # action
        @a amber status
        # then
        assert_output_contains "missing test.txt"

        # action
        @a amber pull b
        @a amber push b
        @b amber sync
        # then
        assert_equal a b
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_new_file_state() -> Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file new.txt "I am new"
        @a amber status
        assert_output_contains "new new.txt"
        @a assert_exists new.txt "I am new"
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_delete_synced_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test-a.txt "Hello A world!"
        @a amber add

        @b amber init b
        @b write_file test-b.txt "Hello B world!"
        @b amber add

        @a amber remote add b local $ROOT/b
        @a amber sync b

        # action 1
        @a amber status
        # then
        assert_output_contains "missing test-b.txt"

        # action 2
        @b amber status
        # then
        assert_output_contains "missing test-a.txt"

        # when
        @a amber pull b
        @a amber status
        @b amber status

        # action 3
        @a amber remove test-a.txt test-b.txt
        @a amber sync b
        @b amber sync

        # then
        assert_equal a b
        @a assert_does_not_exist test-a.txt
        @a assert_does_not_exist test-b.txt
        @b assert_does_not_exist test-a.txt
        @b assert_does_not_exist test-b.txt

        @a amber status
        assert_output_contains "no files detected"
        @b amber status
        assert_output_contains "no files detected"

        @a amber missing
        assert_output_contains "no files missing"
        @b amber missing
        assert_output_contains "no files missing"
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_fsck() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        # action
        @a amber fsck
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rclone_repo_fsck() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello store!"
        @a amber add

        @a amber remote add store rclone :local:/$ROOT/rclone
        @a amber push store

        # action
        @a amber fsck store
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rm() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        # action
        @a amber rm test.txt
        assert_output_contains "deleted test.txt"

        # then
        @a assert_does_not_exist test.txt
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rm_soft() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        # action
        @a amber rm --soft test.txt
        assert_output_contains "deleted [soft] test.txt"

        # then
        @a assert_exists test.txt
        @a amber status
        assert_output_contains "new test.txt"
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rm_not_existing_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        # action
        @a amber rm does-not-exist
        assert_output_contains "already deleted"
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_fs_mv() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        @b amber init b

        @a amber remote add b local $ROOT/b

        # action 1
        @a amber push b
        @b amber sync

        # then 1
        assert_equal a b

        # action 2
        @a amber mv test.txt test.moved
        @a amber sync b
        @b amber sync

        # then 2
        @a assert_exists test.moved "Hello world!"
        @b assert_exists test.moved "Hello world!"
        @a assert_does_not_exist test.txt
        @b assert_does_not_exist test.txt
        assert_equal a b
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_config_set_name() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        # action
        @a amber config set-name b
    "#;
    run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_ssh_connection_sync() -> Result<(), anyhow::Error> {
    let script = r#"
        # Set up repositories
        @a amber init a
        @a write_file test1.txt "Common content!"
        @a write_file test2.txt "Common content!"
        @a amber add

        @b amber init b
        @b write_file test1.txt "Common content!"
        @b amber add

        # Start SSH server for repository A
        @a start_ssh 4567 hunter2

        # Add remote via SSH connection
        @b amber remote add a-ssh ssh "user:hunter2@localhost:4567/"

        # Pull from SSH remote
        @b amber sync a-ssh

        # Verify the file was transferred
        @b assert_exists test2.txt "Common content!"
        assert_equal a b
    "#;
    run_dsl_script(script).await
}
