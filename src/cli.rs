use crate::utils::errors::InternalError;
use crate::{commands, db};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use std::process;

#[derive(Parser)]
#[command(name = "amber")]
#[command(author = "Yasin Zähringer <yasin@yhjz.de>")]
#[command(version = "0.1.0")]
#[command(about = "Distributed blobs", long_about = None)]
pub struct Cli {
    #[arg(long, help = "path to the repository")]
    pub path: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize new repository
    Init {
        #[arg(help = "Name of the new repository")]
        name: String,
    },
    /// Add new files to the repository
    Add {
        #[arg(long, default_value_t = false, help = "Skip file deduplication")]
        skip_deduplication: bool,
        #[arg(long, default_value_t = false, help = "Enable verbose output")]
        verbose: bool,
    },

    /// Display the repository's status.
    Status {
        #[arg(
            long,
            default_value_t = false,
            help = "Display more detailed status information"
        )]
        verbose: bool,
    },
    /// Synchronize the local repository with a remote repository
    Sync {
        #[arg(help = "Remote to synchronize with (default: local)")]
        connection_name: Option<String>,
    },
    /// Pull files from a remote repository
    Pull {
        #[arg(help = "Remote from which to pull files")]
        connection_name: String,
        #[arg(help = "Paths to be pulled (default: all paths)")]
        paths: Vec<PathBuf>,
    },
    /// Push local files to a remote repository
    Push {
        #[arg(help = "Remote to push files to")]
        connection_name: String,
        #[arg(help = "Paths to be pushed (default: all paths)")]
        paths: Vec<PathBuf>,
    },
    /// List missing files in the repository
    Missing {
        #[arg(help = "Show missing files for a remote (default: local)")]
        connection_name: Option<String>,
    },
    /// Remove files from the repository
    #[command(alias = "rm")]
    Remove {
        #[arg(help = "Files to be removed")]
        files: Vec<PathBuf>,
        #[arg(
            long,
            default_value_t = false,
            help = "Keep the file but remove the file from the repository"
        )]
        soft: bool,
    },
    /// Move a tracked file within the repository
    #[command(alias = "mv")]
    Move {
        #[arg(help = "Current path of the file to be moved")]
        source: PathBuf,
        #[arg(help = "New path of the file to be moved")]
        destination: PathBuf,
    },
    /// Run a file system check on the repository
    Fsck {
        #[arg(help = "Fsck a remote (default: local)")]
        connection_name: Option<String>,
    },
    /// Manage remote connections
    Remote {
        #[command(subcommand)]
        command: RemoteCommands,
    },
    /// Configure repository settings
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },
    #[command(hide = true)]
    Serve {},
}

#[derive(Subcommand)]
pub enum RemoteCommands {
    /// Add a new remote connection
    Add {
        #[arg(help = "Name for the remote connection")]
        name: String,
        #[arg(value_enum, help = "Type of remote connection")]
        connection_type: ConnectionType,
        #[arg(
            help = "Parameter for the remote connection (e.g. a path for local, or configuration details for rclone/ssh)"
        )]
        parameter: String,
    },
    /// List all configured remote connections
    List {},
}

#[derive(Subcommand)]
pub enum ConfigCommands {
    SetName {
        #[arg(help = "new name for the repository")]
        name: String,
    },
}

#[derive(Clone, Debug, ValueEnum)]
pub enum ConnectionType {
    Local,
    Rclone,
    Ssh,
}

impl From<ConnectionType> for db::models::ConnectionType {
    fn from(val: ConnectionType) -> Self {
        match val {
            ConnectionType::Local => db::models::ConnectionType::Local,
            ConnectionType::Rclone => db::models::ConnectionType::RClone,
            ConnectionType::Ssh => db::models::ConnectionType::Ssh,
        }
    }
}

pub async fn run() {
    let cli = Cli::parse();

    if let Err(err) = run_cli(cli, crate::flightdeck::output::Output::default()).await {
        eprintln!("\nerror: {}", err);
        process::exit(1);
    }
}

pub async fn run_cli(
    cli: Cli,
    output: crate::flightdeck::output::Output,
) -> Result<(), InternalError> {
    match cli.command {
        Commands::Init { name } => commands::init::init_repository(cli.path, name, output).await,
        Commands::Add {
            skip_deduplication,
            verbose,
        } => commands::add::add(cli.path, skip_deduplication, verbose, output).await,
        Commands::Remove { files, soft } => commands::fs::rm(cli.path, files, soft, output).await,
        Commands::Move {
            source,
            destination,
        } => commands::fs::mv(cli.path, source, destination, output).await,
        Commands::Status { verbose } => commands::status::status(cli.path, verbose, output).await,
        Commands::Missing { connection_name } => {
            commands::missing::missing(cli.path, connection_name, output).await
        }
        Commands::Serve {} => commands::serve::serve(cli.path, output).await,
        Commands::Sync { connection_name } => {
            commands::sync::sync(cli.path, connection_name, output).await
        }
        Commands::Pull {
            connection_name,
            paths,
        } => commands::pull::pull(cli.path, connection_name, paths, output).await,
        Commands::Push {
            connection_name,
            paths,
        } => commands::push::push(cli.path, connection_name, paths, output).await,
        Commands::Fsck { connection_name } => {
            commands::fsck::fsck(cli.path, connection_name, output).await
        }
        Commands::Remote { command } => match command {
            RemoteCommands::Add {
                name,
                connection_type,
                parameter,
            } => {
                commands::remote::add(cli.path, name, connection_type.into(), parameter, output)
                    .await
            }
            RemoteCommands::List {} => commands::remote::list(cli.path, output).await,
        },
        Commands::Config { command } => match command {
            ConfigCommands::SetName { name } => {
                commands::config::set_name(cli.path, name, output).await
            }
        },
    }
}
