mod commands;
mod db;

mod grpc;
mod repository;
mod utils;

use clap::{Parser, Subcommand};
use tokio::time::Instant;

#[derive(Parser)]
#[command(name = "invariant")]
#[command(author = "Yasin Zähringer <yasin-invariable@yhjz.de>")]
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
        #[arg(default_value_t = 50001)]
        port: u16,
    },
    Pull {
        #[arg(default_value_t = 50001)]
        port: u16,
    },
    Export {
        path: String,
    },
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let cli = Cli::parse();
    let start_time = Instant::now();

    match cli.command {
        Commands::Init {} => {
            commands::init::init_repository()
                .await
                .expect("Failed to initialize repository");
        }
        Commands::Add { dry_run } => {
            commands::add::add(dry_run)
                .await
                .expect("Failed to add file to invariable");
        }
        Commands::Status { details } => {
            commands::status::status(details)
                .await
                .expect("Failed to get status of invariable");
        }
        Commands::Missing { files_only } => {
            commands::missing::missing(files_only)
                .await
                .expect("Failed to get status of invariable");
        }
        Commands::Serve { port } => {
            commands::serve::serve(port)
                .await
                .expect("Failed to run server");
        }
        Commands::Sync { port } => {
            commands::sync::sync(port).await.expect("Failed to sync");
        }
        Commands::Pull { port } => {
            commands::pull::pull(port).await.expect("Failed to pull");
        }
        Commands::Export { path } => {
            commands::export::export(path)
                .await
                .expect("Failed to export");
        }
    }
    let duration = start_time.elapsed();
    println!("took: {:.2?}", duration);
}
