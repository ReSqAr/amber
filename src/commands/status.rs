use crate::flightdeck;
use crate::flightdeck::base::{BaseLayoutBuilderBuilder, BaseObserver, TerminationAction};
use crate::flightdeck::base::{StateTransformer, Style};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::logic::state;
use crate::logic::state::VirtualFileState;
use crate::repository::local::LocalRepository;
use crate::repository::traits::{Adder, Config, Local, Metadata, VirtualFilesystem};
use crate::utils::errors::InternalError;
use crate::utils::walker::WalkerConfig;
use futures::StreamExt;
use log::error;
use std::collections::HashMap;
use std::path::PathBuf;

pub async fn status(
    maybe_root: Option<PathBuf>,
    verbose: bool,
    output: flightdeck::output::Output,
) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root).await?;
    let log_path = local_repository.log_path().abs().clone();

    let wrapped = async {
        show_status(local_repository).await?;
        Ok::<(), InternalError>(())
    };

    let terminal = match verbose {
        true => Some(log::LevelFilter::Debug),
        false => None,
    };
    flightdeck::flightdeck(wrapped, root_builders(), log_path, None, terminal, output).await
}

fn root_builders() -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let vfs_refresh = BaseLayoutBuilderBuilder::default()
        .type_key("vfs:refresh")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::Static {
            msg: "refreshing virtual file system...".into(),
            done: "refreshed".into(),
        })
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg}".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

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

    let status = BaseLayoutBuilderBuilder::default()
        .type_key("status")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("done".into()),
                false => msg.unwrap_or("checking files".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({pos})".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    [LayoutItemBuilderNode::from(status).with_children([
        LayoutItemBuilderNode::from(vfs_refresh),
        LayoutItemBuilderNode::from(file),
    ])]
}

#[derive(Eq, Hash, PartialEq, Debug)]
enum State {
    New,
    Missing,
    Ok,
    OkMaterialisationMissing,
    Altered,
    Outdated,
}

impl From<VirtualFileState> for State {
    fn from(vf: VirtualFileState) -> Self {
        match vf {
            VirtualFileState::New => Self::New,
            VirtualFileState::Missing { .. } => Self::Missing,
            VirtualFileState::Ok { .. } => Self::Ok,
            VirtualFileState::OkMaterialisationMissing { .. } => Self::OkMaterialisationMissing,
            VirtualFileState::Altered { .. } => Self::Altered,
            VirtualFileState::Outdated { .. } => Self::Outdated,
        }
    }
}

pub async fn show_status(
    local: impl Metadata + Config + Local + Adder + VirtualFilesystem + Clone + Send + Sync + 'static,
) -> Result<(), InternalError> {
    let start_time = tokio::time::Instant::now();
    let mut checker_obs = BaseObserver::without_id("status");

    let (handle, mut stream) = state::state(local, WalkerConfig::default()).await?;
    let mut count: HashMap<State, i32> = HashMap::new();

    let mut total_count: u64 = 0;
    while let Some(file_result) = stream.next().await {
        total_count += 1;
        checker_obs.observe_position(log::Level::Trace, total_count);

        match file_result {
            Ok(file) => {
                let mut obs = BaseObserver::with_id("file", file.path.clone());

                let state = file.state;
                *count.entry(state.clone().into()).or_insert(0) += 1;
                let (level, state) = match state {
                    VirtualFileState::New => (log::Level::Info, "new"),
                    VirtualFileState::Missing { .. } => (log::Level::Warn, "missing"),
                    VirtualFileState::Outdated { .. } => (log::Level::Info, "outdated"),
                    VirtualFileState::Altered { .. } => (log::Level::Error, "altered"),
                    VirtualFileState::Ok { .. } => (log::Level::Debug, "verified"),
                    VirtualFileState::OkMaterialisationMissing { .. } => (log::Level::Debug, "incomplete"),
                };
                obs.observe_termination(level, state);
            }
            Err(e) => {
                error!("error during traversal: {e}");
            }
        }
    }

    handle.await??;

    let final_msg = generate_final_message(&mut count, start_time);
    checker_obs.observe_termination(log::Level::Info, final_msg);

    Ok(())
}

fn generate_final_message(
    count: &mut HashMap<State, i32>,
    start_time: tokio::time::Instant,
) -> String {
    let new_count = *count.entry(State::New).or_default();
    let missing_count = *count.entry(State::Missing).or_default();
    let outdated_count = *count.entry(State::Outdated).or_default();
    let ok_count = *count.entry(State::Ok).or_default();
    let incomplete_count = *count.entry(State::OkMaterialisationMissing).or_default();
    let altered_count = *count.entry(State::Altered).or_default();

    let mut parts = Vec::new();
    if altered_count > 0 {
        parts.push(format!("{} altered files", altered_count))
    }
    if new_count > 0 {
        parts.push(format!("{} new files", new_count))
    }
    if missing_count > 0 {
        parts.push(format!("{} missing files", missing_count))
    }
    if outdated_count > 0 {
        parts.push(format!("{} outdated files", outdated_count))
    }
    if incomplete_count > 0 {
        parts.push(format!("{} incomplete files", incomplete_count))
    }
    if ok_count > 0 {
        parts.push(format!("{} materialised files", ok_count))
    }
    if !parts.is_empty() {
        let duration = start_time.elapsed();
        format!("detected {} in {duration:.2?}", parts.join(", "))
    } else {
        "no files detected".into()
    }
}
