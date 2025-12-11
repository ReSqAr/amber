use crate::db::models::{BlobAssociatedToFiles, BlobState, ConnectionName, FilesWithAvailability};
use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::repository::local::{LocalRepository, LocalRepositoryConfig};
use crate::repository::traits::{Availability, ConnectionManager, Local};
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::{AppError, InternalError};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio_stream::StreamExt;

pub async fn missing(
    config: LocalRepositoryConfig,
    connection_name: Option<String>,
    summary: Option<usize>,
    output: flightdeck::output::Output,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(config).await?;
    let log_path = local.log_path().abs().clone();

    let wrapped = async {
        if let Some(connection_name) = connection_name {
            let name = ConnectionName(connection_name.clone());
            let connection = local.connect(name).await?;
            let remote = connection.remote.clone();
            match remote {
                WrappedRepository::Local(_) | WrappedRepository::Grpc(_) => {
                    return Err(InternalError::App(AppError::UnsupportedOperation {
                        connection_name,
                        operation: "missing::blobs".into(),
                    }));
                }

                WrappedRepository::RClone(remote) => match summary {
                    None => list_missing_blobs(remote).await?,
                    Some(summary_level) => list_missing_files(remote, summary_level).await?,
                },
            };

            connection.close().await?;
        } else {
            match summary {
                None => list_missing_blobs(local.clone()).await?,
                Some(summary_level) => list_missing_files(local.clone(), summary_level).await?,
            }
        }

        Ok::<(), InternalError>(())
    };

    let wrapped = async {
        let result = wrapped.await;
        local.close().await?;
        result
    };

    flightdeck::flightdeck(wrapped, root_builders(), log_path, None, None, output).await
}

fn root_builders() -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let missing = BaseLayoutBuilderBuilder::default()
        .type_key("missing::blobs")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("done".into()),
                false => msg.unwrap_or("searching".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} {pos}".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    [LayoutItemBuilderNode::from(missing)]
}

pub async fn list_missing_blobs(repository: impl Availability) -> Result<(), InternalError> {
    let start_time = tokio::time::Instant::now();
    let mut missing_obs = BaseObserver::without_id("missing::blobs");

    let mut missing_blobs = repository.missing().await;

    let mut count_files = 0usize;
    let mut count_blobs = 0usize;
    while let Some(blob_result) = missing_blobs.next().await {
        let BlobAssociatedToFiles {
            path,
            blob_id,
            mut repositories_with_blob,
        } = blob_result?;
        count_files += 1;
        count_blobs += 1;
        missing_obs.observe_position(log::Level::Trace, count_blobs as u64);

        repositories_with_blob.sort();
        repositories_with_blob.dedup();
        let detail: String = if !repositories_with_blob.is_empty() {
            format!("(exists in: {})", repositories_with_blob.join(", "))
        } else {
            "(lost - no known location)".into()
        };

        let mut file_obs = BaseObserver::with_id("file", path.0);
        file_obs.observe_termination_ext(
            log::Level::Info,
            "missing",
            [
                ("detail".into(), detail.clone()),
                ("blob_id".into(), blob_id.0.clone()),
                (
                    "repositories_with_blob".into(),
                    repositories_with_blob.join(","),
                ),
            ],
        );
    }

    let final_msg = generate_final_message(count_files, count_blobs, start_time);
    missing_obs.observe_termination(log::Level::Info, final_msg);

    Ok(())
}

fn generate_final_message(
    count_files: usize,
    count_blobs: usize,
    start_time: tokio::time::Instant,
) -> String {
    if count_files > 0 {
        let duration = start_time.elapsed();

        format!(
            "detected {count_files} missing files and {count_blobs} missing blobs in {duration:.2?}"
        )
    } else {
        "no files missing".into()
    }
}

async fn list_missing_files(
    repository: impl Availability,
    level: usize,
) -> Result<(), InternalError> {
    let start_time = tokio::time::Instant::now();

    let mut missing_obs = BaseObserver::without_id("missing::files");

    let mut files = repository.current_files_with_availability().await;

    let mut agg = HashMap::<_, (u64, u64)>::new();
    while let Some(f) = files.try_next().await? {
        let FilesWithAvailability {
            path, blob_state, ..
        } = f;
        let missing_inc = match blob_state {
            BlobState::Present => 0,
            BlobState::Missing => 1,
        };

        let mut key = truncate_path(path.as_ref(), level)?;
        loop {
            let path = key.clone();
            let v = match agg.get(&path) {
                Some((m, t)) => (m + missing_inc, t + 1),
                None => (missing_inc, 1),
            };
            agg.insert(path, v);

            key = match key.parent() {
                None => break,
                Some(key) => key.to_owned(),
            };
        }
    }

    let root_total = agg.remove(&PathBuf::from(""));
    let mut agg: Vec<_> = agg.into_iter().collect();
    agg.sort_unstable_by(|(p1, _), (p2, _)| p1.cmp(p2));

    for (path, (missing, total)) in agg {
        if missing > 0 {
            let msg = format!(
                "{}/ {missing} missing ({total} total)",
                path.to_string_lossy()
            );
            missing_obs.observe_termination(log::Level::Info, msg);
        }
    }

    let duration = start_time.elapsed();
    let final_msg = match root_total {
        Some((missing, total)) => {
            format!("detected {missing} missing files out of {total} files in {duration:.2?}.")
        }
        None => format!("no files detected in {duration:.2?}."),
    };
    missing_obs.observe_termination(log::Level::Info, final_msg);

    Ok(())
}

fn truncate_path(path: &str, level: usize) -> Result<PathBuf, InternalError> {
    match std::path::Path::new(path).parent() {
        Some(parent) => Ok(parent.components().take(level).collect()),
        None => Err(InternalError::InvariantError(
            "parent returns None for relative path".to_string(),
        )),
    }
}
