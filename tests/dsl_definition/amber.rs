use crate::dsl_definition::writer::ChannelWriter;
use amber::cli::{Cli, run_cli};
use amber::flightdeck::output::Output;
use clap::Parser;
use std::env;
use std::io::Write;
use std::path::Path;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;

pub(crate) async fn run_amber_cli_command(
    args: &[String],
    working_dir: &Path,
    root: &Path,
) -> anyhow::Result<String, anyhow::Error> {
    let substituted: Vec<String> = args
        .iter()
        .map(|arg| arg.replace("$ROOT", &root.to_string_lossy()))
        .collect();

    let mut cli = Cli::try_parse_from(&substituted)?;
    cli.path = Some(working_dir.to_path_buf());

    let (tx, rx): (UnboundedSender<Vec<u8>>, UnboundedReceiver<Vec<u8>>) = unbounded_channel();
    let writer = ChannelWriter::new(tx);
    let writer = std::sync::Arc::new(std::sync::Mutex::new(
        Box::new(writer) as Box<dyn Write + Send + Sync>
    ));

    let current_dur = env::current_dir()?;
    env::set_current_dir(working_dir)?;
    run_cli(cli, Output::Override(writer)).await?;
    env::set_current_dir(current_dur)?;

    let chunks: Vec<Vec<u8>> = UnboundedReceiverStream::new(rx).collect().await;
    let combined: Vec<u8> = chunks.into_iter().flatten().collect();
    let output: String = String::from_utf8_lossy(&combined).into();
    println!(
        "{}",
        output
            .lines()
            .map(|l| format!("     > {}", l))
            .collect::<Vec<String>>()
            .join("\n")
    );

    Ok(output)
}
