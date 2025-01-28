use crate::utils::errors::InternalError;
use log::debug;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

#[derive(Debug, Clone)]
pub struct LocalConfig {
    pub path: PathBuf,
}

impl LocalConfig {
    fn to_rclone_arg(&self) -> String {
        self.path.display().to_string()
    }

    fn to_config_section(&self) -> Option<String> {
        None
    }
}
#[derive(Debug, Clone)]
pub struct SshConfig {
    pub remote_name: String,
    pub user: String,
    pub host: String,
    pub remote_path: PathBuf,
    pub port: Option<u16>,
}

impl SshConfig {
    fn to_rclone_arg(&self) -> String {
        format!("{}:{}", self.remote_name, self.remote_path.display())
    }
    fn to_config_section(&self) -> Option<String> {
        let mut section = format!(
            "[{}]\n\
                     type = sftp\n\
                     host = {}\n\
                     user = {}\n",
            self.remote_name, self.host, self.user
        );
        if let Some(port) = self.port {
            section.push_str(&format!("port = {}\n", port));
        }
        Some(section)
    }
}

#[derive(Debug, Clone)]
pub enum RcloneTarget {
    Local(LocalConfig),
    Ssh(SshConfig),
}

impl RcloneTarget {
    pub fn to_rclone_arg(&self) -> String {
        match self {
            RcloneTarget::Local(cfg) => cfg.to_rclone_arg(),
            RcloneTarget::Ssh(cfg) => cfg.to_rclone_arg(),
        }
    }

    pub fn to_config_section(&self) -> Option<String> {
        match self {
            RcloneTarget::Local(cfg) => cfg.to_config_section(),
            RcloneTarget::Ssh(cfg) => cfg.to_config_section(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RcloneStats {
    #[serde(default)]
    pub bytes: u64,

    #[serde(default)]
    pub elapsed_time: f64,

    #[serde(default)]
    pub errors: u64,

    #[serde(default)]
    pub eta: Option<String>,

    #[serde(default)]
    pub fatal_error: bool,

    #[serde(default)]
    pub retry_error: bool,

    #[serde(default)]
    pub speed: u64,

    #[serde(default)]
    pub total_bytes: u64,

    #[serde(default)]
    pub total_transfers: u64,

    #[serde(default)]
    pub transfer_time: f64,

    #[serde(default)]
    pub transfers: u64,
}

#[derive(Debug, Deserialize)]
struct RcloneJsonLog {
    level: String,
    msg: String,

    #[serde(default)]
    stats: Option<RcloneStats>,
}

#[derive(Debug)]
pub enum RcloneEvent {
    Message(String),
    Error(String),
    Stats(RcloneStats),
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
        .arg("--log-level")
        .arg("INFO")
        .arg(&source_arg)
        .arg(&dest_arg)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    debug!("rclone command: {:?}", command);

    let mut child = command.spawn()?;

    if let Some(stdout) = child.stdout.take() {
        let mut reader = BufReader::new(stdout).lines();

        while let Some(line) = reader.next_line().await? {
            debug!("rclone stdout: {line}");
            match serde_json::from_str::<RcloneJsonLog>(&line) {
                Ok(json_log) => {
                    debug!("rclone json log: {json_log:?}");
                    let event = match (json_log.level.as_str(), json_log.stats) {
                        ("error", _) => RcloneEvent::Error(json_log.msg),
                        (_, Some(stats)) => RcloneEvent::Stats(stats),
                        (_, None) => RcloneEvent::Message(json_log.msg),
                    };
                    callback(event);
                }
                Err(_parse_err) => {
                    // TODO: debug stmt
                    // If parsing fails, skip or log it. We won't call the callback for unknown lines.
                }
            }
        }
    }

    //
    // 6. Handle stderr lines // TODO: if stderr is not buffered, this might hang; but in general, better to have that last
    //
    if let Some(stderr) = child.stderr.take() {
        let mut reader = BufReader::new(stderr).lines();
        while let Some(line) = reader.next_line().await? {
            // We treat stderr lines as messages, but you could treat them differently
            callback(RcloneEvent::Message(format!("[stderr] {line}")));
        }
    }

    let status = child.wait().await?;
    if !status.success() {
        return Err(InternalError::RClone(status.code().unwrap_or(-1)));
    }

    Ok(())
}
