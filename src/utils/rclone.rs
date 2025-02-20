use crate::utils::errors::InternalError;
use aes::cipher::{KeyIvInit, StreamCipher};
use base64::Engine;
use log::debug;
use serde::Deserialize;
use std::path::Path;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

#[derive(Debug, Clone)]
pub struct LocalConfig {
    pub path: String,
}

impl LocalConfig {
    fn to_rclone_arg(&self) -> String {
        self.path.clone()
    }

    fn to_config_section(&self) -> Option<String> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct RCloneConfig {
    pub remote_name: String,
    pub config: String,
    pub remote_path: String,
}

impl RCloneConfig {
    fn to_rclone_arg(&self) -> String {
        format!("{}:{}", self.remote_name, self.remote_path)
    }

    fn to_config_section(&self) -> Option<String> {
        Some(format!("[{}]\n{}\n", self.remote_name, self.config))
    }
}

#[derive(Clone, Debug)]
pub enum SshAuth {
    Password(String),
    Agent,
}

#[derive(Debug, Clone)]
pub struct SshConfig {
    pub remote_name: String,
    pub host: String,
    pub port: Option<u16>,
    pub user: String,
    pub auth: SshAuth,
    pub remote_path: String,
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

impl SshConfig {
    fn to_rclone_arg(&self) -> String {
        format!("{}:{}", self.remote_name, self.remote_path)
    }

    fn to_config_section(&self) -> Option<String> {
        let SshConfig {
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
        Some(lines.join("\n"))
    }
}

#[derive(Debug, Clone)]
pub enum RcloneTarget {
    Local(LocalConfig),
    RClone(RCloneConfig),
    Ssh(SshConfig),
}

impl RcloneTarget {
    pub fn to_rclone_arg(&self) -> String {
        match self {
            RcloneTarget::Local(cfg) => cfg.to_rclone_arg(),
            RcloneTarget::RClone(cfg) => cfg.to_rclone_arg(),
            RcloneTarget::Ssh(cfg) => cfg.to_rclone_arg(),
        }
    }

    pub fn to_config_section(&self) -> Option<String> {
        match self {
            RcloneTarget::Local(cfg) => cfg.to_config_section(),
            RcloneTarget::RClone(cfg) => cfg.to_config_section(),
            RcloneTarget::Ssh(cfg) => cfg.to_config_section(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RcloneStats {
    #[serde(default)]
    pub bytes: u64,
    pub total_bytes: u64,
    pub speed: f64,
    pub eta: Option<f64>,
    #[serde(default)]
    pub transferring: Vec<TransferProgress>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferProgress {
    pub bytes: u64,
    pub name: String,
    pub size: u64,
}

#[derive(Debug, Deserialize)]
struct RcloneJsonLog {
    level: String,
    msg: String,

    #[serde(default)]
    object: Option<String>,

    #[serde(default)]
    stats: Option<RcloneStats>,
}

#[derive(Debug)]
pub enum RcloneEvent {
    UnknownMessage(String),
    Error(String),
    Stats(RcloneStats),
    Copied(String),
    Ok(String),
    Fail(String),
    #[allow(dead_code)]
    UnknownDebugMessage(String),
}

pub enum Operation {
    Copy,
    Check,
}

impl Operation {
    pub fn to_rclone_arg(&self) -> String {
        match self {
            Operation::Copy => "copy".into(),
            Operation::Check => "check".into(),
        }
    }
}

pub async fn run_rclone<F>(
    operation: Operation,
    temp_path: &Path,
    file_list_path: &Path,
    source: RcloneTarget,
    destination: RcloneTarget,
    mut callback: F,
) -> Result<(), InternalError>
where
    F: FnMut(RcloneEvent) + Send + 'static,
{
    let mut sections = Vec::new();
    if let Some(s) = source.to_config_section() {
        sections.push(s);
    }
    if let Some(s) = destination.to_config_section() {
        sections.push(s);
    }

    let config_contents = sections.join("\n");
    let config_path = temp_path.join("rclone.conf");
    {
        let file = fs::File::create(&config_path).await?;
        let mut writer = tokio::io::BufWriter::new(file);
        writer.write_all(config_contents.as_bytes()).await?;
        writer.flush().await?;
    }

    let source_arg = source.to_rclone_arg();
    let dest_arg = destination.to_rclone_arg();

    let log_level = match operation {
        Operation::Copy => "INFO",
        Operation::Check => "DEBUG",
    };

    let mut command = Command::new("rclone");
    command
        .arg(operation.to_rclone_arg())
        .arg("--config")
        .arg(&config_path)
        .arg("--files-from")
        .arg(file_list_path.display().to_string())
        .arg("--use-json-log")
        .arg("--stats")
        .arg("1s")
        .arg("--log-level")
        .arg(log_level)
        .arg(&source_arg)
        .arg(&dest_arg)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    debug!("rclone command: {command:?}");

    let mut child = command.spawn()?;

    // Take ownership of stdout and stderr
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    // Create BufReaders for stdout and stderr
    let mut stdout_reader = match stdout {
        Some(out) => BufReader::new(out).lines(),
        None => {
            return Err(InternalError::Stream(
                "could not read stdout of rclone".to_string(),
            ));
        }
    };

    let mut stderr_reader = match stderr {
        Some(err) => BufReader::new(err).lines(),
        None => {
            return Err(InternalError::Stream(
                "could not read stderr of rclone".to_string(),
            ));
        }
    };

    loop {
        tokio::select! {
            // Read a line from stdout
            stdout_line = stdout_reader.next_line() => {
                match stdout_line {
                    Ok(Some(line)) => {
                        callback(parse_rclone_line("stdout", &line));
                    },
                    Ok(None) => {
                        break;
                    },
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            },

            stderr_line = stderr_reader.next_line() => {
                match stderr_line {
                    Ok(Some(line)) => {
                        callback(parse_rclone_line("stderr", &line));
                    },
                    Ok(None) => {
                        break;
                    },
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            },

            else => {
                break;
            }
        }
    }

    let status = child.wait().await?;
    if !status.success() {
        return Err(InternalError::RClone(status.code().unwrap_or(-1)));
    }

    Ok(())
}

fn parse_rclone_line(channel: &str, line: &str) -> RcloneEvent {
    match serde_json::from_str::<RcloneJsonLog>(line) {
        Ok(json_log) => {
            if "error" == json_log.level {
                if let Some(object) = json_log.object {
                    RcloneEvent::Fail(object)
                } else {
                    RcloneEvent::Error(json_log.msg)
                }
            } else if let Some(stats) = json_log.stats {
                RcloneEvent::Stats(stats)
            } else if json_log.msg.starts_with("Copied") {
                if let Some(object) = json_log.object {
                    RcloneEvent::Copied(object)
                } else {
                    RcloneEvent::UnknownMessage(line.into())
                }
            } else if "OK" == json_log.msg {
                if let Some(object) = json_log.object {
                    RcloneEvent::Ok(object)
                } else {
                    RcloneEvent::UnknownMessage(line.into())
                }
            } else if "debug" == json_log.level {
                RcloneEvent::UnknownDebugMessage(line.into())
            } else {
                RcloneEvent::UnknownMessage(line.into())
            }
        }
        Err(parse_err) => {
            debug!("Failed to parse rclone {channel} JSON: {parse_err}");
            RcloneEvent::UnknownMessage(line.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::fs;

    #[tokio::test]
    async fn test_parse_rclone_line_error_without_object() {
        let json_line = r#"{"level": "error", "msg": "Something went wrong"}"#;
        let event = parse_rclone_line("stdout", json_line);
        match event {
            RcloneEvent::Error(msg) => assert_eq!(msg, "Something went wrong"),
            _ => panic!("Expected RcloneEvent::Error, got {:?}", event),
        }
    }

    #[tokio::test]
    async fn test_parse_rclone_line_error_with_object() {
        let json_line = r#"{"level": "error", "msg": "Error occurred", "object": "file.txt"}"#;
        let event = parse_rclone_line("stderr", json_line);
        match event {
            RcloneEvent::Fail(object) => assert_eq!(object, "file.txt"),
            _ => panic!("Expected RcloneEvent::Fail, got {:?}", event),
        }
    }

    #[tokio::test]
    async fn test_parse_rclone_line_stats() {
        let json_line = r#"{
            "level": "info",
            "msg": "",
            "stats": {
                "bytes": 1024,
                "totalBytes": 2048,
                "eta": 5.0,
                "transferring": [{"bytes": 512, "name": "file1.txt", "size": 1024}]
            }
        }"#;
        let event = parse_rclone_line("stdout", json_line);
        match event {
            RcloneEvent::Stats(stats) => {
                assert_eq!(stats.bytes, 1024);
                assert_eq!(stats.total_bytes, 2048);
                assert_eq!(stats.eta, Some(5.0));
                assert_eq!(stats.transferring.len(), 1);
                let tp = &stats.transferring[0];
                assert_eq!(tp.name, "file1.txt");
                assert_eq!(tp.bytes, 512);
                assert_eq!(tp.size, 1024);
            }
            _ => panic!("Expected RcloneEvent::Stats, got {:?}", event),
        }
    }

    #[tokio::test]
    async fn test_parse_rclone_line_copied() {
        let json_line = r#"{"level": "info", "msg": "Copied", "object": "file2.txt"}"#;
        let event = parse_rclone_line("stdout", json_line);
        match event {
            RcloneEvent::Copied(object) => assert_eq!(object, "file2.txt"),
            _ => panic!("Expected RcloneEvent::Copied, got {:?}", event),
        }
    }

    #[tokio::test]
    async fn test_parse_rclone_line_ok() {
        let json_line = r#"{"level": "info", "msg": "OK", "object": "file3.txt"}"#;
        let event = parse_rclone_line("stdout", json_line);
        match event {
            RcloneEvent::Ok(object) => assert_eq!(object, "file3.txt"),
            _ => panic!("Expected RcloneEvent::Ok, got {:?}", event),
        }
    }

    #[tokio::test]
    async fn test_parse_rclone_line_debug() {
        let json_line = r#"{"level": "debug", "msg": "This is a debug message"}"#;
        let event = parse_rclone_line("stderr", json_line);
        match event {
            RcloneEvent::UnknownDebugMessage(msg) => {
                assert!(msg.contains("This is a debug message"))
            }
            _ => panic!("Expected RcloneEvent::UnknownDebugMessage, got {:?}", event),
        }
    }

    #[tokio::test]
    async fn test_parse_rclone_line_invalid_json() {
        let invalid_line = "not a json string";
        let event = parse_rclone_line("stdout", invalid_line);
        match event {
            RcloneEvent::UnknownMessage(msg) => assert_eq!(msg, invalid_line),
            _ => panic!("Expected RcloneEvent::UnknownMessage, got {:?}", event),
        }
    }

    #[tokio::test]
    async fn test_rclone_copy_local_to_local_single_file() -> Result<(), InternalError> {
        let base_dir = tempdir().expect("failed to create temp dir");
        let base_path = base_dir.path();

        let source_dir = base_path.join("source");
        let dest_dir = base_path.join("dest");
        fs::create_dir_all(&source_dir).await?;
        fs::create_dir_all(&dest_dir).await?;

        let source_file = source_dir.join("test.txt");
        let file_content = b"Hello from rclone test";
        fs::write(&source_file, file_content).await?;

        let file_list_path = base_path.join("filelist.txt");
        let mut file_list = fs::File::create(&file_list_path).await?;
        file_list.write_all(b"test.txt\n").await?;
        file_list.flush().await?;

        let rclone_temp_dir = base_path.join("rclone_temp");
        fs::create_dir_all(&rclone_temp_dir).await?;

        let source_config = LocalConfig {
            path: source_dir.to_string_lossy().into_owned(),
        };
        let dest_config = LocalConfig {
            path: dest_dir.to_string_lossy().into_owned(),
        };

        let source_target = RcloneTarget::Local(source_config);
        let dest_target = RcloneTarget::Local(dest_config);

        let callback = |_event: RcloneEvent| {};

        // run rclone
        run_rclone(
            Operation::Copy,
            &rclone_temp_dir,
            &file_list_path,
            source_target,
            dest_target,
            callback,
        )
        .await
        .expect("rclone copy failed");

        // check that the file now exists in the destination directory
        let dest_file = dest_dir.join("test.txt");
        let metadata = fs::metadata(&dest_file).await;
        assert!(
            metadata.is_ok(),
            "Destination file was not created: {:?}",
            dest_file
        );

        // check that the copied file's content matches.
        let copied_content = fs::read(&dest_file).await?;
        assert_eq!(
            copied_content, file_content,
            "The copied file content does not match the source."
        );

        Ok(())
    }
}
