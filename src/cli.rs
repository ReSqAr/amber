use crate::utils::errors::InternalError;
use crate::{commands, db};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use std::process;

#[derive(Parser)]
#[command(name = "amber")]
#[command(author = "Yasin Zähringer <yasin-amber@yhjz.de>")]
#[command(version = "0.1.0")]
#[command(about = "Distributed blobs", long_about = None)]
pub struct Cli {
    #[arg(long, help = "path to the repository")]
    pub path: Option<PathBuf>,

    #[arg(
        hide = true,
        env = "AMBER_REPOSITORY_FOLDER_NAME",
        default_value = ".amb"
    )]
    pub app_folder: PathBuf,

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
        #[arg(long, default_value_t = 30, help = "rclone's transfer parallelism")]
        rclone_transfers: usize,
        #[arg(long, default_value_t = 30, help = "rclone's check parallelism")]
        rclone_checkers: usize,
    },
    /// Push local files to a remote repository
    Push {
        #[arg(help = "Remote to push files to")]
        connection_name: String,
        #[arg(help = "Paths to be pushed (default: all paths)")]
        paths: Vec<PathBuf>,
        #[arg(long, default_value_t = 30, help = "rclone's transfer parallelism")]
        rclone_transfers: usize,
        #[arg(long, default_value_t = 30, help = "rclone's check parallelism")]
        rclone_checkers: usize,
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
        #[arg(long, default_value_t = false, help = "Also remove the file from disk")]
        hard: bool,
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
        #[arg(long, default_value_t = 30, help = "rclone's check parallelism")]
        rclone_checkers: usize,
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

    let output = crate::flightdeck::output::Output::default();
    init_logger(&output);

    if let Err(err) = run_cli(cli, output).await {
        log::error!("\nerror: {}", err);
        process::exit(1);
    }
}

pub async fn run_cli(
    cli: Cli,
    output: crate::flightdeck::output::Output,
) -> Result<(), InternalError> {
    match cli.command {
        Commands::Init { name } => {
            commands::init::init_repository(cli.path, cli.app_folder, name, output).await
        }
        Commands::Add { verbose } => {
            commands::add::add(cli.path, cli.app_folder, verbose, output).await
        }
        Commands::Remove { files, hard } => {
            commands::fs::rm(cli.path, cli.app_folder, files, hard, output).await
        }
        Commands::Move {
            source,
            destination,
        } => commands::fs::mv(cli.path, cli.app_folder, source, destination, output).await,
        Commands::Status { verbose } => {
            commands::status::status(cli.path, cli.app_folder, verbose, output).await
        }
        Commands::Missing { connection_name } => {
            commands::missing::missing(cli.path, cli.app_folder, connection_name, output).await
        }
        Commands::Serve {} => commands::serve::serve(cli.path, cli.app_folder, output).await,
        Commands::Sync { connection_name } => {
            commands::sync::sync(cli.path, cli.app_folder, connection_name, output).await
        }
        Commands::Pull {
            connection_name,
            paths,
            rclone_transfers,
            rclone_checkers,
        } => {
            commands::pull::pull(
                cli.path,
                cli.app_folder,
                connection_name,
                paths,
                output,
                rclone_transfers,
                rclone_checkers,
            )
            .await
        }
        Commands::Push {
            connection_name,
            paths,
            rclone_transfers,
            rclone_checkers,
        } => {
            commands::push::push(
                cli.path,
                cli.app_folder,
                connection_name,
                paths,
                output,
                rclone_transfers,
                rclone_checkers,
            )
            .await
        }
        Commands::Fsck {
            connection_name,
            rclone_checkers,
        } => {
            commands::fsck::fsck(
                cli.path,
                cli.app_folder,
                connection_name,
                output,
                rclone_checkers,
            )
            .await
        }
        Commands::Remote { command } => match command {
            RemoteCommands::Add {
                name,
                connection_type,
                parameter,
            } => {
                commands::remote::add(
                    cli.path,
                    cli.app_folder,
                    name,
                    connection_type.into(),
                    parameter,
                    output,
                )
                .await
            }
            RemoteCommands::List {} => {
                commands::remote::list(cli.path, cli.app_folder, output).await
            }
        },
        Commands::Config { command } => match command {
            ConfigCommands::SetName { name } => {
                commands::config::set_name(cli.path, cli.app_folder, name, output).await
            }
        },
    }
}

fn init_logger(output: &crate::flightdeck::output::Output) {
    let env = env_logger::Env::default().default_filter_or("info");
    let mut builder = env_logger::Builder::from_env(env);

    if let Some(m) = output.multi_progress_bar() {
        let logger = builder.build();
        let level = logger.filter();
        indicatif_log_bridge::LogWrapper::new(m.clone(), logger)
            .try_init()
            .unwrap();
        log::set_max_level(level);
    } else {
        builder.init()
    }
}
