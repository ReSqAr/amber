use serde::Deserialize;
use std::error::Error;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use tempfile::NamedTempFile;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

// ---------------------------------------------
// Rclone targets (source/destination)
// ---------------------------------------------

// A high-level enum describing where files come from or go to:
// - Local disk
// - SSH (SFTP)
// - S3
// - A raw, custom remote spec if you need something not covered by the above
#[derive(Debug, Clone)]
pub enum RcloneTarget {
    Local(LocalConfig),
    Ssh(SshConfig),
    S3(S3Config),
    // Escape hatch: if you already have a fully configured remote in an
    // external config or you want to specify something like "myRemote:folder"
    // directly, you can do so here.
    Raw(RemoteRawSpec),
}

// Minimal config for a "local" path.
#[derive(Debug, Clone)]
pub struct LocalConfig {
    // The local directory path
    pub path: PathBuf,
}

// Minimal config for an SSH (SFTP) remote.
// Typically, you'd define a "remote_name" that must match
// the `[remote_name]` section in the rclone config.
#[derive(Debug, Clone)]
pub struct SshConfig {
    pub remote_name: String,

    // The SSH user and host, e.g., "user" and "hostname"
    pub user: String,
    pub host: String,

    // Remote path on that host
    pub remote_path: PathBuf,

    // (Optional) custom port, key file, etc. could go here
    pub port: Option<u16>,
}

// Minimal config for an S3 bucket remote.
#[derive(Debug, Clone)]
pub struct S3Config {
    // The name that will appear in `[this_name]` in rclone config
    pub remote_name: String,

    // The S3 bucket name, e.g., "mybucket"
    pub bucket: String,

    // The region, e.g., "us-east-1"
    pub region: String,

    // Path within that bucket
    pub remote_path: PathBuf,

    // Access key ID (optional if using environment variables or IAM roles)
    pub access_key_id: Option<String>,

    // Secret access key (optional if using environment variables or IAM roles)
    pub secret_access_key: Option<String>,
}

// A fully formed remote spec, such as `"someRemote:subfolder"`.
#[derive(Debug, Clone)]
pub struct RemoteRawSpec {
    pub remote_spec: String,
}

impl RcloneTarget {
    // Generate a single-line string that rclone expects for either the source or destination.
    // Example outputs:
    //   - Local => "/home/myuser/files"
    //   - SSH => "my_ssh_remote:/home/remoteuser/files"
    //   - S3  => "my_s3_remote:backup_folder"
    //   - Raw => "whatever:stuff"
    pub fn to_rclone_arg(&self) -> String {
        match self {
            RcloneTarget::Local(cfg) => cfg.path.display().to_string(),
            RcloneTarget::Ssh(cfg) => format!("{}:{}", cfg.remote_name, cfg.remote_path.display()),
            RcloneTarget::S3(cfg) => format!("{}:{}", cfg.remote_name, cfg.remote_path.display()),
            RcloneTarget::Raw(raw) => raw.remote_spec.clone(),
        }
    }

    // Return a snippet of config for this remote if we need to define one.
    // Local config doesn't need a dedicated remote section.
    // Raw might not need one if the user uses a pre-existing remote name.
    // In that case, simply return None. Otherwise, we generate something minimal.
    //
    // Note: In real usage, you might want to handle advanced SSH/S3 options,
    // credentials, environment, etc.
    pub fn to_config_section(&self) -> Option<String> {
        match self {
            // Local paths do NOT require a separate [local] section in rclone config.
            RcloneTarget::Local(_) => None,

            RcloneTarget::Ssh(cfg) => {
                // Example minimal SFTP remote config
                //
                // [my_ssh_remote]
                // type = sftp
                // host = ...
                // user = ...
                // port = ...
                let mut section = format!(
                    "[{}]\n\
                     type = sftp\n\
                     host = {}\n\
                     user = {}\n",
                    cfg.remote_name, cfg.host, cfg.user
                );
                if let Some(port) = cfg.port {
                    section.push_str(&format!("port = {}\n", port));
                }
                Some(section)
            }

            RcloneTarget::S3(cfg) => {
                // Minimal S3 config
                //
                // [my_s3_remote]
                // type = s3
                // provider = AWS
                // env_auth = false
                // access_key_id = ...
                // secret_access_key = ...
                // region = ...
                //
                // In a real app, you'd store credentials securely or pass them in from outside.
                // This is purely illustrative.

                // Prepare credentials if provided
                let access_key = cfg.access_key_id.clone().unwrap_or_default();
                let secret_key = cfg.secret_access_key.clone().unwrap_or_default();

                let section = format!(
                    "[{}]\n\
                     type = s3\n\
                     provider = AWS\n\
                     env_auth = false\n\
                     access_key_id = {}\n\
                     secret_access_key = {}\n\
                     region = {}\n\
                     location_constraint = {}\n\
                     acl = private\n",
                    cfg.remote_name, access_key, secret_key, cfg.region, cfg.region
                );
                Some(section)
            }

            RcloneTarget::Raw(_) => {
                // The user presumably already has the remote defined in some
                // external config or in their own text. We won't generate anything.
                None
            }
        }
    }
}

// ---------------------------------------------
// The rclone operation (upload, download, sync, etc.)
// ---------------------------------------------
#[derive(Debug, Clone, Copy)]
pub enum RcloneOperation {
    Copy,
    Sync,
    Move,
    // Add more as needed
}

impl RcloneOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            RcloneOperation::Copy => "copy",
            RcloneOperation::Sync => "sync",
            RcloneOperation::Move => "move",
        }
    }
}

// ---------------------------------------------
// Progress / logging events from rclone
// ---------------------------------------------

// A structured event we can produce for the user's callback.
#[derive(Debug)]
pub enum RcloneEvent {
    // A generic message line.
    Message(String),

    // An error message line.
    Error(String),

    // A progress event (e.g. upload or download progress).
    Progress(RcloneProgress),
}

// A minimal progress structure. rclone's JSON can contain more/different fields.
#[derive(Debug, Deserialize)]
pub struct RcloneProgress {
    #[serde(default)]
    pub percentage: f64,

    #[serde(default)]
    pub speed: f64,

    #[serde(default)]
    pub name: String,
}

// The raw JSON lines from `--use-json-log`.
// We convert these into our `RcloneEvent`.
#[derive(Debug, Deserialize)]
struct RcloneJsonLog {
    level: String,
    msg: String,

    #[serde(default)]
    progress: Option<RcloneProgress>,
}

// ---------------------------------------------
// Main function to run an rclone operation
// ---------------------------------------------
pub async fn run_rclone_operation<F>(
    operation: RcloneOperation,
    source: RcloneTarget,
    destination: RcloneTarget,
    files: Vec<PathBuf>,
    extra_args: Vec<String>,
    mut callback: F,
) -> Result<(), Box<dyn Error>>
where
    F: FnMut(RcloneEvent) + Send + 'static,
{
    //
    // 1. Generate an rclone config from the source & destination if needed
    //
    //    We combine the config sections from both targets into one file.
    //    For example, if source is local and destination is s3, we'll just
    //    produce the s3 remote block. If either is raw or local, we might skip.
    //
    let mut sections = Vec::new();
    if let Some(s) = source.to_config_section() {
        sections.push(s);
    }
    if let Some(s) = destination.to_config_section() {
        sections.push(s);
    }

    let config_contents = sections.join("\n");
    let mut config_file = NamedTempFile::new()?;
    {
        let mut writer = BufWriter::new(&mut config_file);
        writer.write_all(config_contents.as_bytes())?;
        writer.flush()?;
    }
    let config_path = config_file.path().to_path_buf();

    //
    // 2. Prepare a file list, if you want rclone to only process certain files
    //    within that source. Typically, these paths should be relative to the
    //    source if it's local. If you pass absolute paths, rclone will attempt
    //    to interpret them as absolute on your local filesystem (which might
    //    skip the "source" path. For S3 sources, these should be relative to the bucket's root or specified path.
    //
    let mut file_list = NamedTempFile::new()?;
    {
        let mut writer = BufWriter::new(&mut file_list);
        for path in &files {
            writeln!(writer, "{}", path.display())?;
        }
        writer.flush()?;
    }
    let file_list_path = file_list.path().to_path_buf();

    //
    // 3. Construct the rclone command with desired operation
    //
    let source_arg = source.to_rclone_arg();
    let dest_arg = destination.to_rclone_arg();

    let mut command = Command::new("rclone");
    command
        .arg(operation.as_str())
        // Force rclone to use our generated config
        .arg("--config")
        .arg(&config_path)
        // Only include files listed in file_list
        .arg("--files-from")
        .arg(&file_list_path)
        // JSON logging
        .arg("--use-json-log")
        .arg("--log-level")
        .arg("INFO")
        // Source and Destination
        .arg(&source_arg)
        .arg(&dest_arg)
        // Possibly user-supplied arguments
        .args(&extra_args)
        // We'll pipe both stdout and stderr for parsing
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    //
    // 4. Spawn the process
    //
    let mut child = command.spawn()?;

    //
    // 5. Parse JSON from stdout line by line
    //
    if let Some(stdout) = child.stdout.take() {
        let mut reader = BufReader::new(stdout).lines();

        while let Some(line) = reader.next_line().await? {
            match serde_json::from_str::<RcloneJsonLog>(&line) {
                Ok(json_log) => {
                    let event = match (json_log.level.as_str(), json_log.progress) {
                        ("error", _) => RcloneEvent::Error(json_log.msg),
                        (_, Some(progress)) => RcloneEvent::Progress(progress),
                        (_, None) => RcloneEvent::Message(json_log.msg),
                    };
                    callback(event);
                }
                Err(_parse_err) => {
                    // If parsing fails, skip or log it. We won't call the callback for unknown lines.
                }
            }
        }
    }

    //
    // 6. Handle stderr lines
    //
    if let Some(stderr) = child.stderr.take() {
        let mut reader = BufReader::new(stderr).lines();
        while let Some(line) = reader.next_line().await? {
            // We treat stderr lines as messages, but you could treat them differently
            callback(RcloneEvent::Message(format!("[stderr] {}", line)));
        }
    }

    //
    // 7. Wait for completion and check exit code
    //
    let status = child.wait().await?;
    if !status.success() {
        return Err(format!("rclone exited with code {}", status).into());
    }

    Ok(())
}

// ---------------------------------------------
// Example usage with S3 as the source
// ---------------------------------------------
async fn test() -> Result<(), Box<dyn Error>> {
    // Example: S3 -> Local "copy" operation (download).
    // We'll specify a minimal S3 config and a local folder path.

    // Define the source as S3
    let source = RcloneTarget::S3(S3Config {
        remote_name: "my_s3_remote".to_string(),
        bucket: "my-bucket".to_string(),
        region: "us-east-1".to_string(),
        remote_path: "data".into(), // e.g., "data/" within the bucket
        access_key_id: Some("YOUR_ACCESS_KEY_ID".to_string()),
        secret_access_key: Some("YOUR_SECRET_ACCESS_KEY".to_string()),
    });

    // Define the destination as local
    let destination = RcloneTarget::Local(LocalConfig {
        path: "/path/to/local/download/folder".into(),
    });

    // Suppose we only want to download these specific files from S3:
    // Relative to "data/" path in the bucket
    let files_to_download = vec![
        PathBuf::from("file1.txt"),
        PathBuf::from("images/photo.png"),
    ];

    // Our callback that receives progress info or messages
    let callback = |event: RcloneEvent| match event {
        RcloneEvent::Message(msg) => println!("[Message] {}", msg),
        RcloneEvent::Error(err) => eprintln!("[Error] {}", err),
        RcloneEvent::Progress(progress) => {
            println!(
                "[Progress] name={}, percentage={:.2}%, speed={:.2} B/s",
                progress.name, progress.percentage, progress.speed
            );
        }
    };

    // Perform a copy (download from S3 to local)
    run_rclone_operation(
        RcloneOperation::Copy,
        source,
        destination,
        files_to_download,
        vec![], // extra rclone args
        callback,
    )
    .await?;

    // Example 2: Upload from local to S3 using the same function.
    let local_source = RcloneTarget::Local(LocalConfig {
        path: "/path/to/local/upload/folder".into(),
    });

    let s3_destination = RcloneTarget::S3(S3Config {
        remote_name: "my_s3_remote".to_string(),
        bucket: "my-bucket".to_string(),
        region: "us-east-1".to_string(),
        remote_path: "backup".into(),
        access_key_id: Some("YOUR_ACCESS_KEY_ID".to_string()),
        secret_access_key: Some("YOUR_SECRET_ACCESS_KEY".to_string()),
    });

    let files_to_upload = vec![
        PathBuf::from("document.pdf"),
        PathBuf::from("videos/movie.mp4"),
    ];

    run_rclone_operation(
        RcloneOperation::Copy,
        local_source,
        s3_destination,
        files_to_upload,
        vec![], // extra rclone args
        |event| println!("Upload Event: {:?}", event),
    )
    .await?;

    Ok(())
}
