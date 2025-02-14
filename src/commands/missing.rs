use crate::db::models::BlobWithPaths;
use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::repository::local::LocalRepository;
use crate::repository::traits::{Local, Missing};
use crate::utils::errors::InternalError;
use log::error;
use std::path::PathBuf;
use tokio::time::Instant;
use tokio_stream::StreamExt;

pub async fn missing(maybe_root: Option<PathBuf>) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root).await?;
    let log_path = local_repository.log_path().abs().clone();

    let wrapped = async {
        list_missing_blobs(local_repository).await?;
        Ok::<(), InternalError>(())
    };

    flightdeck::flightdeck(wrapped, root_builders(), log_path, None, None).await
}

fn root_builders() -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let missing = BaseLayoutBuilderBuilder::default()
        .type_key("missing")
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

pub async fn list_missing_blobs(repository: impl Missing) -> Result<(), InternalError> {
    let start_time = tokio::time::Instant::now();
    let mut missing_obs = BaseObserver::without_id("missing");

    let mut missing_blobs = repository.missing();

    let mut count_files = 0usize;
    let mut count_blobs = 0usize;
    while let Some(blob_result) = missing_blobs.next().await {
        match blob_result {
            Ok(BlobWithPaths {
                paths,
                blob_id,
                mut repositories_with_blob,
            }) => {
                count_files += paths.len();
                count_blobs += 1;
                missing_obs.observe_position(log::Level::Trace, count_blobs as u64);

                repositories_with_blob.sort();
                repositories_with_blob.dedup();

                for path in paths {
                    let mut file_obs = BaseObserver::with_id("file", path);
                    file_obs.observe_termination_ext(
                        log::Level::Info,
                        "missing",
                        [
                            ("blob_id".into(), blob_id.clone()),
                            (
                                "repositories_with_blob".into(),
                                repositories_with_blob.join(","),
                            ),
                        ],
                    );
                }
            }
            Err(e) => {
                error!("error during traversal: {e}");
            }
        }
    }

    let final_msg = generate_final_message(count_files, count_blobs, start_time);
    missing_obs.observe_termination(log::Level::Info, final_msg);

    Ok(())
}

fn generate_final_message(count_files: usize, count_blobs: usize, start_time: Instant) -> String {
    if count_files > 0 {
        let duration = start_time.elapsed();

        format!(
            "detected {count_files} missing files and {count_blobs} missing blobs in {duration:.2?}"
        )
    } else {
        "no files missing".into()
    }
}
