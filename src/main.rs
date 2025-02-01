mod cli;
mod commands;
mod db;
mod flightdeck;
mod grpc;
mod repository;
mod utils;

use tokio::time::Instant;

#[tokio::main]
async fn main() {
    env_logger::init();

    let start_time = Instant::now();

    cli::run().await;
    let duration = start_time.elapsed();
    println!("took: {:.2?}", duration);
}
