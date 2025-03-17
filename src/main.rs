mod cli;
mod commands;
mod connection;
mod db;
mod flightdeck;
mod grpc;
mod logic;
mod repository;
mod utils;

#[tokio::main]
async fn main() {
    cli::run().await;
}
