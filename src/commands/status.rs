use crate::db::models::VirtualFileState;
use crate::flightdeck;
use crate::flightdeck::base::{BaseLayoutBuilderBuilder, TerminationAction};
use crate::flightdeck::base::{BaseObservable, BaseObservation};
use crate::flightdeck::base::{StateTransformer, Style};
use crate::flightdeck::observer::Observer;
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::repository::local::LocalRepository;
use crate::repository::logic::state;
use crate::repository::traits::{Adder, Config, Local, Metadata, VirtualFilesystem};
use crate::utils::errors::InternalError;
use crate::utils::walker::WalkerConfig;
use futures::StreamExt;
use log::error;
use std::collections::HashMap;
use std::path::PathBuf;

pub async fn status(maybe_root: Option<PathBuf>, verbose: bool) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root).await?;
    let log_path = local_repository.log_path().abs().into();

    let wrapped = async {
        show_status(local_repository).await?;
        Ok::<(), InternalError>(())
    };

    let terminal = match verbose {
        true => Some(log::LevelFilter::Debug),
        false => None,
    };
    flightdeck::flightdeck(wrapped, root_builders(), log_path, None, terminal).await
}

fn root_builders() -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let file = BaseLayoutBuilderBuilder::default()
        .type_key("file")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdFn(Box::new(move |done, id| {
            let path = id.unwrap_or("<missing>".into());
            match done {
                true => format!("checked {}", path),
                false => format!("checking {}", path),
            }
        })))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg}".into(),
            done: "{prefix}{spinner:.green} {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let overall = BaseLayoutBuilderBuilder::default()
        .type_key("checker")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("done".into()),
                false => msg.unwrap_or("checking".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} {pos}".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    [LayoutItemBuilderNode::from(overall).add_child(file)]
}

pub async fn show_status(
    local: impl Metadata + Config + Local + Adder + VirtualFilesystem + Clone + Send + Sync + 'static,
) -> Result<(), InternalError> {
    let start_time = tokio::time::Instant::now();
    let mut checker_obs = Observer::new(BaseObservable::without_id("checker"));

    let (handle, mut stream) = state::state(local, WalkerConfig::default()).await?;
    let mut count = HashMap::new();

    let mut total_count: u64 = 0;
    while let Some(file_result) = stream.next().await {
        total_count += 1;
        checker_obs.observe(log::Level::Trace, BaseObservation::Position(total_count));

        match file_result {
            Ok(file) => {
                let mut obs = Observer::new(BaseObservable::with_id("file", file.path.clone()));

                let state = file.state.unwrap_or(VirtualFileState::NeedsCheck);
                *count.entry(state.clone()).or_insert(0) += 1;
                let (level, state) = match state {
                    VirtualFileState::New => (log::Level::Info, "new"),
                    VirtualFileState::Deleted => (log::Level::Warn, "deleted"),
                    VirtualFileState::Dirty | VirtualFileState::NeedsCheck => {
                        (log::Level::Error, "broken")
                    }
                    VirtualFileState::Ok => (log::Level::Debug, "verified"),
                };
                obs.observe(level, BaseObservation::TerminalState(state.into()));
            }
            Err(e) => {
                error!("error during traversal: {e}");
            }
        }
    }

    handle.await??;

    let final_msg = generate_final_message(&mut count, start_time);
    checker_obs.observe(log::Level::Info, BaseObservation::TerminalState(final_msg));

    Ok(())
}

fn generate_final_message(
    count: &mut HashMap<VirtualFileState, i32>,
    start_time: tokio::time::Instant,
) -> String {
    let new_count = *count.entry(VirtualFileState::New).or_default();
    let deleted_count = *count.entry(VirtualFileState::Deleted).or_default();
    let ok_count = *count.entry(VirtualFileState::Ok).or_default();
    let dirty_count = *count.entry(VirtualFileState::Dirty).or_default();
    let needs_check_count = *count.entry(VirtualFileState::NeedsCheck).or_default();

    let mut parts = Vec::new();
    if new_count > 0 {
        parts.push(format!("{} new files", new_count))
    }
    if deleted_count > 0 {
        parts.push(format!("{} deleted files", deleted_count))
    }
    if ok_count > 0 {
        parts.push(format!("{} verified files", ok_count))
    }
    if dirty_count + needs_check_count > 0 {
        parts.push(format!("{} broken files", dirty_count + needs_check_count))
    }
    if !parts.is_empty() {
        let duration = start_time.elapsed();
        format!("{} in {duration:.2?}", parts.join(" "))
    } else {
        "no files detected".into()
    }
}
