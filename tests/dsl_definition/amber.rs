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
    expected_failure: Option<String>,
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

    let current_dur =
        env::current_dir().inspect_err(|e| log::error!("unable to get current directory: {e}"))?;
    env::set_current_dir(working_dir).inspect_err(|e| {
        log::error!("unable to change current directory to {working_dir:?}: {e}")
    })?;
    let result = run_cli(cli, Output::Override(writer))
        .await
        .inspect_err(|e| log::error!("cli run failed: {e}"));
    env::set_current_dir(&current_dur).inspect_err(|e| {
        log::error!("unable to change current directory back to {current_dur:?}: {e}")
    })?;

    let chunks: Vec<Vec<u8>> = UnboundedReceiverStream::new(rx).collect().await;
    let combined: Vec<u8> = chunks.into_iter().flatten().collect();
    let output: String = String::from_utf8_lossy(&combined).into();
    let output = output
        .lines()
        .map(|l| format!("     > {}", l))
        .collect::<Vec<String>>()
        .join("\n");
    if !output.is_empty() {
        println!("{}", output);
    } else {
        println!("<no output>");
    }

    match (result, expected_failure) {
        (Ok(_), None) => {
            println!("amber command was successful");
        }
        (Err(e), Some(exp)) => {
            if !e.to_string().contains(&exp) {
                return Err(anyhow::anyhow!(
                    "unexpected failure\n  cause: {e}\n  expected: {exp}"
                ));
            }
            println!("amber failed as expected\n  cause: {e}");
        }
        (Ok(_), Some(exp)) => {
            return Err(anyhow::anyhow!(
                "amber unexpectedly succeeded - expected {exp}"
            ));
        }
        (Err(e), None) => return Err(anyhow::anyhow!("amber unexpectedly failed with {e}")),
    }

    Ok(output)
}
