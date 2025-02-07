use crate::{commands, db};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "amber")]
#[command(author = "Yasin Zähringer <yasin@yhjz.de>")]
#[command(version = "1.0")]
#[command(about = "distribute blobs", long_about = None)]
struct Cli {
    #[arg(long)]
    path: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init {},
    Add {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        verbose: bool,
    },
    Status {
        #[arg(long, default_value_t = false)]
        verbose: bool,
    },
    Missing {},
    #[command(hide = true)]
    Serve {},
    Sync {
        connection_name: Option<String>,
    },
    Pull {
        connection_name: String,
    },
    Push {
        connection_name: String,
    },
    Remote {
        #[command(subcommand)]
        command: RemoteCommands,
    },
}

#[derive(Subcommand)]
enum RemoteCommands {
    Add {
        name: String,
        #[arg(value_enum)]
        connection_type: ConnectionType,
        parameter: String,
    },

    List {},
}

#[derive(Clone, Debug, ValueEnum)]
enum ConnectionType {
    Local,
    Ssh,
}

impl From<ConnectionType> for db::models::ConnectionType {
    fn from(val: ConnectionType) -> Self {
        match val {
            ConnectionType::Local => db::models::ConnectionType::Local,
            ConnectionType::Ssh => db::models::ConnectionType::Ssh,
        }
    }
}

pub async fn run() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init {} => {
            commands::init::init_repository(cli.path)
                .await
                .expect("Failed to initialize repository");
        }
        Commands::Add { dry_run, verbose } => {
            commands::add::add(cli.path, dry_run, verbose)
                .await
                .expect("Failed to add file");
        }
        Commands::Status { verbose } => {
            commands::status::status(cli.path, verbose)
                .await
                .expect("Failed to get status");
        }
        Commands::Missing {} => {
            commands::missing::missing(cli.path)
                .await
                .expect("Failed to get missing");
        }
        Commands::Serve {} => {
            commands::serve::serve(cli.path)
                .await
                .expect("Failed to run server");
        }
        Commands::Sync { connection_name } => {
            commands::sync::sync(cli.path, connection_name)
                .await
                .expect("Failed to sync");
        }
        Commands::Pull { connection_name } => {
            commands::pull::pull(cli.path, connection_name)
                .await
                .expect("Failed to pull");
        }
        Commands::Push { connection_name } => {
            commands::push::push(cli.path, connection_name)
                .await
                .expect("Failed to push");
        }
        Commands::Remote { command } => match command {
            RemoteCommands::Add {
                name,
                connection_type,
                parameter,
            } => {
                commands::remote::add(cli.path, name, connection_type.into(), parameter)
                    .await
                    .expect("Failed to add remote");
            }
            RemoteCommands::List {} => {
                commands::remote::list(cli.path)
                    .await
                    .expect("Failed to list remotes");
            }
        },
    }
}
