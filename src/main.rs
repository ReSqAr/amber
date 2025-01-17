mod commands;
mod db;

mod transport;

use clap::{Parser, Subcommand};

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
        path: String,
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

    match cli.command {
        Commands::Init {} => {
            commands::init::init_repository()
                .await
                .expect("Failed to initialize repository");
        }
        Commands::Add { path } => {
            commands::add::add_file(path)
                .await
                .expect("Failed to add file to invariable");
        }
        Commands::Serve { port } => {
            commands::serve::serve(port)
                .await
                .expect("Failed to run server");
        }
        Commands::Sync { port } => {
            commands::sync::sync(port)
                .await
                .expect("Failed to sync");
        }
        Commands::Pull { port } => {
            commands::pull::pull(port)
                .await
                .expect("Failed to pull");
        }
        Commands::Export { path } => {
            commands::export::export(path)
                .await
                .expect("Failed to export");
        }
    }
}
