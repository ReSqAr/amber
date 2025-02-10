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
    /// Initialize the repository
    Init {},
    /// Add files to the repository
    Add {
        #[arg(long, default_value_t = false)]
        skip_deduplication: bool,
        #[arg(long, default_value_t = false)]
        verbose: bool,
    },
    /// Show status information
    Status {
        #[arg(long, default_value_t = false)]
        verbose: bool,
    },
    /// Show missing files
    Missing {},
    /// (Hidden) Serve command
    #[command(hide = true)]
    Serve {},
    /// Sync with a remote connection
    Sync { connection_name: Option<String> },
    /// Pull data from a remote connection
    Pull { connection_name: String },
    /// Push data to a remote connection
    Push { connection_name: String },
    /// Manage remotes
    Remote {
        #[command(subcommand)]
        command: RemoteCommands,
    },
}

#[derive(Subcommand)]
enum RemoteCommands {
    /// Add a new remote
    Add {
        name: String,
        #[arg(value_enum)]
        connection_type: ConnectionType,
        parameter: String,
    },
    /// List all remotes
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

    let result = match cli.command {
        Commands::Init {} => commands::init::init_repository(cli.path).await,
        Commands::Add {
            skip_deduplication,
            verbose,
        } => commands::add::add(cli.path, skip_deduplication, verbose).await,
        Commands::Status { verbose } => commands::status::status(cli.path, verbose).await,
        Commands::Missing {} => commands::missing::missing(cli.path).await,
        Commands::Serve {} => commands::serve::serve(cli.path).await,
        Commands::Sync { connection_name } => commands::sync::sync(cli.path, connection_name).await,
        Commands::Pull { connection_name } => commands::pull::pull(cli.path, connection_name).await,
        Commands::Push { connection_name } => commands::push::push(cli.path, connection_name).await,
        Commands::Remote { command } => match command {
            RemoteCommands::Add {
                name,
                connection_type,
                parameter,
            } => commands::remote::add(cli.path, name, connection_type.into(), parameter).await,
            RemoteCommands::List {} => commands::remote::list(cli.path).await,
        },
    };

    if let Err(err) = result {
        eprintln!("\nan error occurred: {}", err);
        process::exit(1);
    }
}
