use crate::utils::errors::InternalError;
use aes::cipher::{KeyIvInit, StreamCipher};
use base64::Engine;
use log::debug;
use serde::Deserialize;
use std::path::Path;
use tokio::fs::File;
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
    pub name: String,
    pub config: String,
    pub remote_path: String,
}

impl RCloneConfig {
    fn to_rclone_arg(&self) -> String {
        format!("{}:{}", self.name, self.remote_path) 
    }

    fn to_config_section(&self) -> Option<String> {
        Some(format!("[{}]\n{}\n", self.name, self.config))
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
}

pub async fn run_rclone_operation<F>(
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
        let file = File::create(&config_path).await?;
        let mut writer = tokio::io::BufWriter::new(file);
        writer.write_all(config_contents.as_bytes()).await?;
        writer.flush().await?;
    }

    let source_arg = source.to_rclone_arg();
    let dest_arg = destination.to_rclone_arg();

    let mut command = Command::new("rclone");
    command
        .arg("copy")
        .arg("--config")
        .arg(&config_path)
        .arg("--files-from")
        .arg(file_list_path.display().to_string())
        .arg("--use-json-log")
        .arg("--stats")
        .arg("1s")
        .arg("--log-level")
        .arg("INFO")
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

    let parse_and_callback = |channel: &str, line: String, callback: &mut F| {
        match serde_json::from_str::<RcloneJsonLog>(&line) {
            Ok(json_log) => {
                let event = if "error" == json_log.level.as_str() {
                    RcloneEvent::Error(json_log.msg)
                } else if let Some(stats) = json_log.stats {
                    RcloneEvent::Stats(stats)
                } else if json_log.msg.starts_with("Copied") {
                    if let Some(object) = json_log.object {
                        RcloneEvent::Copied(object)
                    } else {
                        RcloneEvent::UnknownMessage(line)
                    }
                } else {
                    RcloneEvent::UnknownMessage(line)
                };
                callback(event);
            }
            Err(parse_err) => {
                debug!("Failed to parse rclone {channel} JSON: {parse_err}");
                callback(RcloneEvent::UnknownMessage(line));
            }
        }
    };

    loop {
        tokio::select! {
            // Read a line from stdout
            stdout_line = stdout_reader.next_line() => {
                match stdout_line {
                    Ok(Some(line)) => {
                        parse_and_callback("stdout", line, &mut callback);
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
                        parse_and_callback("stderr", line, &mut callback);
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
