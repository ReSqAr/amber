use crate::utils::errors::{AppError, InternalError};
use futures::TryStreamExt;
use log::debug;
use serde::Deserialize;
use std::fmt::Debug;
use std::path::Path;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio_stream::wrappers::LinesStream;

pub enum ConfigSection {
    None,
    Config(String),
    GlobalConfig,
}

pub trait RCloneTarget: Clone {
    fn to_rclone_arg(&self) -> String;
    fn to_config_section(&self) -> ConfigSection;
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RcloneStats {
    #[serde(default)]
    pub bytes: u64,
    pub total_bytes: u64,
    pub speed: Option<f64>,
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
    UnchangedSkipping(String),
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

pub async fn check_rclone() -> Result<(), InternalError> {
    match Command::new("rclone").arg("--version").output().await {
        Ok(output) => {
            if output.status.success() {
                debug!(
                    "rclone --version output: {}",
                    String::from_utf8_lossy(&output.stdout)
                );
                Ok(())
            } else {
                Err(AppError::RCloneErr(format!(
                    "rclone failed during check (error: {})",
                    String::from_utf8_lossy(&output.stderr)
                ))
                .into())
            }
        }
        Err(e) => Err(AppError::RCloneErr(e.to_string()).into()),
    }
}

#[derive(Default)]
pub struct RCloneConfig {
    pub(crate) transfers: Option<usize>,
    pub(crate) checkers: Option<usize>,
}

pub async fn run_rclone<F>(
    operation: Operation,
    temp_path: &Path,
    file_list_path: &Path,
    source: impl RCloneTarget,
    destination: impl RCloneTarget,
    config: RCloneConfig,
    mut callback: F,
) -> Result<(), InternalError>
where
    F: FnMut(RcloneEvent) + Send + 'static,
{
    let source_config_section = source.to_config_section();
    let destination_config_section = destination.to_config_section();
    let config_path = if matches!(source_config_section, ConfigSection::Config(_))
        || matches!(destination_config_section, ConfigSection::Config(_))
    {
        let mut sections = Vec::new();
        match source_config_section {
            ConfigSection::Config(c) => sections.push(c),
            ConfigSection::None => {}
            ConfigSection::GlobalConfig => return Err(InternalError::RCloneMixedConfig),
        }
        match destination_config_section {
            ConfigSection::Config(c) => sections.push(c),
            ConfigSection::None => {}
            ConfigSection::GlobalConfig => return Err(InternalError::RCloneMixedConfig),
        }

        let config_path = temp_path.join("rclone.conf");
        let file = fs::File::create(&config_path).await?;
        let mut writer = tokio::io::BufWriter::new(file);
        writer.write_all(sections.join("\n").as_bytes()).await?;
        writer.flush().await?;
        Some(config_path)
    } else {
        None
    };

    let source_arg = source.to_rclone_arg();
    let dest_arg = destination.to_rclone_arg();

    let mut command = Command::new("rclone");
    command.arg(operation.to_rclone_arg());
    if let Some(config_path) = config_path {
        command.arg("--config").arg(&config_path);
    }
    if let Some(transfers) = config.transfers {
        command.arg(format!("--transfers={transfers}"));
    }
    if let Some(checkers) = config.checkers {
        command.arg(format!("--checkers={checkers}"));
    }
    command
        .arg("--files-from")
        .arg(file_list_path.display().to_string())
        .arg("--use-json-log")
        .arg("--stats")
        .arg("1s")
        .arg("--log-level")
        .arg("DEBUG")
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
    let stdout_reader = match stdout {
        Some(out) => BufReader::new(out).lines(),
        None => {
            return Err(InternalError::Stream(
                "could not read stdout of rclone".to_string(),
            ));
        }
    };

    let stderr_reader = match stderr {
        Some(err) => BufReader::new(err).lines(),
        None => {
            return Err(InternalError::Stream(
                "could not read stderr of rclone".to_string(),
            ));
        }
    };

    let stdout_stream = LinesStream::new(stdout_reader).map_ok(|line| ("stdout", line));
    let stderr_stream = LinesStream::new(stderr_reader).map_ok(|line| ("stderr", line));
    let mut merged = futures::stream::select(stdout_stream, stderr_stream);
    while let Some((which, line)) = merged.try_next().await? {
        callback(parse_rclone_line(which, &line));
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
            } else if json_log.msg.to_lowercase().starts_with("copied")
                || json_log
                    .msg
                    .to_lowercase()
                    .starts_with("multi-thread copied")
            {
                if let Some(object) = json_log.object {
                    RcloneEvent::Copied(object)
                } else {
                    RcloneEvent::UnknownMessage(line.into())
                }
            } else if json_log
                .msg
                .to_lowercase()
                .starts_with("unchanged skipping")
            {
                if let Some(object) = json_log.object {
                    RcloneEvent::UnchangedSkipping(object)
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

#[allow(clippy::indexing_slicing, clippy::wildcard_enum_match_arm)]
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
    async fn test_parse_rclone_line_multi_threaded_copied() {
        let json_line =
            r#"{"level": "info", "msg": "Multi-thread Copied (new)", "object": "file2.txt"}"#;
        let event = parse_rclone_line("stdout", json_line);
        match event {
            RcloneEvent::Copied(object) => assert_eq!(object, "file2.txt"),
            _ => panic!("Expected RcloneEvent::Copied, got {:?}", event),
        }
    }

    #[tokio::test]
    async fn test_parse_rclone_line_unchanged_skipping() {
        let json_line = r#"{"level": "info", "msg": "Unchanged skipping", "object": "file2.txt"}"#;
        let event = parse_rclone_line("stdout", json_line);
        match event {
            RcloneEvent::UnchangedSkipping(object) => assert_eq!(object, "file2.txt"),
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

    #[derive(Debug, Clone)]
    pub struct LocalTarget {
        pub path: String,
    }

    impl RCloneTarget for LocalTarget {
        fn to_rclone_arg(&self) -> String {
            self.path.clone()
        }

        fn to_config_section(&self) -> ConfigSection {
            ConfigSection::None
        }
    }

    #[tokio::test]
    async fn test_rclone_copy_local_to_local_single_file() -> Result<(), InternalError> {
        let base_dir = tempdir().expect("failed to create temp dir");
        let base_path = base_dir.path();

        let source_dir = base_path.join("source");
        let dest_dir = base_path.join("dest");
        fs::create_dir_all(&source_dir)
            .await
            .expect("failed to create source dir");
        fs::create_dir_all(&dest_dir)
            .await
            .expect("failed to create dest dir");

        let source_file = source_dir.join("test.txt");
        let file_content = b"Hello from rclone test";
        fs::write(&source_file, file_content)
            .await
            .expect("failed to write to source file");

        let file_list_path = base_path.join("filelist.txt");
        let mut file_list = fs::File::create(&file_list_path)
            .await
            .expect("failed to create file list");
        file_list
            .write_all(b"test.txt\n")
            .await
            .expect("failed to write to filelist");
        file_list
            .flush()
            .await
            .expect("failed to write to filelist");

        let rclone_temp_dir = base_path.join("rclone_temp");
        fs::create_dir_all(&rclone_temp_dir)
            .await
            .expect("failed to create rclone temp dir");

        let source_target = LocalTarget {
            path: source_dir.to_string_lossy().into_owned(),
        };
        let dest_target = LocalTarget {
            path: dest_dir.to_string_lossy().into_owned(),
        };

        let callback = |_event: RcloneEvent| {};

        // run rclone
        run_rclone(
            Operation::Copy,
            &rclone_temp_dir,
            &file_list_path,
            source_target,
            dest_target,
            RCloneConfig {
                transfers: Some(1),
                checkers: Some(1),
            },
            callback,
        )
        .await
        .expect("run_rclone failed");

        // check that the file now exists in the destination directory
        let dest_file = dest_dir.join("test.txt");
        let metadata = fs::metadata(&dest_file).await;
        assert!(
            metadata.is_ok(),
            "Destination file was not created: {:?}",
            dest_file
        );

        // check that the copied file's content matches.
        let copied_content = fs::read(&dest_file)
            .await
            .expect("failed to read source file");
        assert_eq!(
            copied_content, file_content,
            "The copied file content does not match the source."
        );

        Ok(())
    }
}
