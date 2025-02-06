mod cli;
mod commands;
mod db;
mod flightdeck;
mod grpc;
mod repository;
mod utils;

#[tokio::main]
async fn main() {
    env_logger::init();

    cli::run().await;
}
