use crate::db::models::VirtualFileState;
use crate::flightdeck;
use crate::flightdeck::base::{BaseLayoutBuilderBuilder, TerminationAction};
use crate::flightdeck::base::{BaseObservable, BaseObservation};
use crate::flightdeck::base::{StateTransformer, Style};
use crate::flightdeck::observer::Observer;
use crate::flightdeck::progress_manager::LayoutItemBuilderNode;
use crate::repository::local::LocalRepository;
use crate::repository::logic::state;
use crate::repository::traits::{Adder, Config, Local, Metadata, VirtualFilesystem};
use crate::utils::errors::InternalError;
use crate::utils::walker::WalkerConfig;
use futures::StreamExt;
use log::error;
use std::collections::HashMap;
use std::path::PathBuf;

pub async fn status(maybe_root: Option<PathBuf>, details: bool) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root).await?;
    let log_path = local_repository.log_path().abs().into();

    let wrapped = async {
        show_status(local_repository, details).await?;
        Ok::<(), InternalError>(())
    };

    flightdeck::flightdeck(wrapped, root_builders(), log_path, log::LevelFilter::Info).await
}

fn root_builders() -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let file = BaseLayoutBuilderBuilder::default()
        .type_key("file")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg}".into(),
            done: "{prefix}{spinner:.green} {msg}".into(),
        })
        .build()
        .expect("build should work")
        .boxed();

    let overall = BaseLayoutBuilderBuilder::default()
        .type_key("checker")
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
        .build()
        .expect("build should work")
        .boxed();

    [LayoutItemBuilderNode::from(overall).add_child(file)]
}

pub async fn show_status(
    local: impl Metadata + Config + Local + Adder + VirtualFilesystem + Clone + Send + Sync + 'static,
    details: bool,
) -> Result<(), InternalError> {
    let mut checker_obs = Observer::new(BaseObservable::without_id("checker"));

    let (handle, mut stream) = state::state(local, WalkerConfig::default()).await?;
    let mut count = HashMap::new();

    let mut total_count: u64 = 0;
    while let Some(file_result) = stream.next().await {
        total_count += 1;
        checker_obs.observe(log::Level::Trace, BaseObservation::Position(total_count));

        match file_result {
            Ok(file) => {
                let _ = Observer::with_auto_termination(
                    BaseObservable::with_id("file", file.path.clone()),
                    log::Level::Info,
                    BaseObservation::TerminalState("done".into()),
                );

                let state = file.state.unwrap_or(VirtualFileState::NeedsCheck);
                *count.entry(state.clone()).or_insert(0) += 1;
                if details {
                    match state {
                        VirtualFileState::New => {
                            println!("new: {}", file.path);
                        }
                        VirtualFileState::Deleted => {
                            println!("deleted: {}", file.path);
                        }
                        VirtualFileState::Dirty | VirtualFileState::NeedsCheck => {
                            println!("BROKEN: {}", file.path);
                        }
                        VirtualFileState::Ok => {}
                    }
                }
            }
            Err(e) => {
                error!("error during traversal: {e}");
            }
        }
    }

    handle.await??;

    let final_msg = generate_final_message(&mut count);
    checker_obs.observe(log::Level::Info, BaseObservation::TerminalState(final_msg));

    Ok(())
}

fn generate_final_message(count: &mut HashMap<VirtualFileState, i32>) -> String {
    let new_count = *count.entry(VirtualFileState::New).or_default();
    let deleted_count = *count.entry(VirtualFileState::Deleted).or_default();
    let ok_count = *count.entry(VirtualFileState::Ok).or_default();
    let dirty_count = *count.entry(VirtualFileState::Dirty).or_default();
    let needs_check_count = *count.entry(VirtualFileState::NeedsCheck).or_default();

    let mut parts = Vec::new();
    if new_count > 0 {
        parts.push(format!("new: {}", new_count))
    }
    if deleted_count > 0 {
        parts.push(format!("deleted: {}", deleted_count))
    }
    if ok_count > 0 {
        parts.push(format!("ok: {}", ok_count))
    }
    if dirty_count + needs_check_count > 0 {
        parts.push(format!("broken: {}", dirty_count + needs_check_count))
    }
    if !parts.is_empty() {
        parts.join(" ")
    } else {
        "no files detected".into()
    }
}
