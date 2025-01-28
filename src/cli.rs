use crate::{commands, db};
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "amber")]
#[command(author = "Yasin Zähringer <yasin@yhjz.de>")]
#[command(version = "1.0")]
#[command(about = "distribute blobs", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init {},
    Add {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    Status {
        #[arg(long, default_value_t = false)]
        details: bool,
    },
    Missing {
        #[arg(long, default_value_t = false)]
        files_only: bool,
    },
    Serve {
        #[arg(default_value_t = 50001)]
        port: u16,
    },
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
}

impl From<ConnectionType> for db::models::ConnectionType {
    fn from(val: ConnectionType) -> Self {
        match val {
            ConnectionType::Local => db::models::ConnectionType::Local,
        }
    }
}

pub async fn run() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init {} => {
            commands::init::init_repository()
                .await
                .expect("Failed to initialize repository");
        }
        Commands::Add { dry_run } => {
            commands::add::add(dry_run)
                .await
                .expect("Failed to add file");
        }
        Commands::Status { details } => {
            commands::status::status(details)
                .await
                .expect("Failed to get status");
        }
        Commands::Missing { files_only } => {
            commands::missing::missing(files_only)
                .await
                .expect("Failed to get missing");
        }
        Commands::Serve { port } => {
            commands::serve::serve(port)
                .await
                .expect("Failed to run server");
        }
        Commands::Sync { connection_name } => {
            commands::sync::sync(connection_name)
                .await
                .expect("Failed to sync");
        }
        Commands::Pull { connection_name } => {
            commands::pull::pull(connection_name)
                .await
                .expect("Failed to pull");
        }
        Commands::Push { connection_name } => {
            commands::push::push(connection_name)
                .await
                .expect("Failed to push");
        }
        Commands::Remote { command } => match command {
            RemoteCommands::Add {
                name,
                connection_type,
                parameter,
            } => {
                commands::remote::add(name, connection_type.into(), parameter)
                    .await
                    .expect("Failed to add remote");
            }
            RemoteCommands::List {} => {
                commands::remote::list()
                    .await
                    .expect("Failed to list remotes");
            }
        },
    }
}
