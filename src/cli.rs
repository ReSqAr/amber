use crate::{commands, db};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use std::process;

#[derive(Parser)]
#[command(name = "amber")]
#[command(author = "Yasin Zähringer <yasin@yhjz.de>")]
#[command(version = "1.0")]
#[command(about = "distribute blobs", long_about = None)]
struct Cli {
    /// Optional path to the repository
    #[arg(long)]
    path: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init {
        name: String,
    },
    Add {
        #[arg(long, default_value_t = false)]
        skip_deduplication: bool,
        #[arg(long, default_value_t = false)]
        verbose: bool,
    },
    #[command(alias = "rm")]
    Remove {
        files: Vec<PathBuf>,
        #[arg(long, default_value_t = false)]
        soft: bool,
    },
    #[command(alias = "mv")]
    Move {
        source: PathBuf,
        destination: PathBuf,
    },
    Status {
        #[arg(long, default_value_t = false)]
        verbose: bool,
    },
    Missing {
        connection_name: Option<String>,
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
    Fsck {},
    Remote {
        #[command(subcommand)]
        command: RemoteCommands,
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },
    #[command(hide = true)]
    Serve {},
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

#[derive(Subcommand)]
enum ConfigCommands {
    Name { name: String },
}

#[derive(Clone, Debug, ValueEnum)]
enum ConnectionType {
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

    let result = match cli.command {
        Commands::Init { name } => commands::init::init_repository(cli.path, name).await,
        Commands::Add {
            skip_deduplication,
            verbose,
        } => commands::add::add(cli.path, skip_deduplication, verbose).await,
        Commands::Remove { files, soft } => commands::fs::rm(cli.path, files, soft).await,
        Commands::Move {
            source,
            destination,
        } => commands::fs::mv(cli.path, source, destination).await,
        Commands::Status { verbose } => commands::status::status(cli.path, verbose).await,
        Commands::Missing { connection_name } => {
            commands::missing::missing(cli.path, connection_name).await
        }
        Commands::Serve {} => commands::serve::serve(cli.path).await,
        Commands::Sync { connection_name } => commands::sync::sync(cli.path, connection_name).await,
        Commands::Pull { connection_name } => commands::pull::pull(cli.path, connection_name).await,
        Commands::Push { connection_name } => commands::push::push(cli.path, connection_name).await,
        Commands::Fsck {} => commands::fsck::fsck(cli.path).await,
        Commands::Remote { command } => match command {
            RemoteCommands::Add {
                name,
                connection_type,
                parameter,
            } => commands::remote::add(cli.path, name, connection_type.into(), parameter).await,
            RemoteCommands::List {} => commands::remote::list(cli.path).await,
        },
        Commands::Config { command } => match command {
            ConfigCommands::Name { name } => commands::config::set_name(cli.path, name).await,
        },
    };

    if let Err(err) = result {
        eprintln!("\nerror: {}", err);
        process::exit(1);
    }
}
