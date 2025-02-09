use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::repository::local::LocalRepository;
use crate::repository::logic::transfer::{cleanup_staging, transfer};
use crate::repository::traits::{ConnectionManager, Local};
use crate::utils::errors::InternalError;
use std::path::PathBuf;

pub async fn push(
    maybe_root: Option<PathBuf>,
    connection_name: String,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(maybe_root).await?;
    let log_path = local.log_path().abs().clone();

    let wrapped = async {
        let start_time = tokio::time::Instant::now();
        let mut sync_obs = BaseObserver::without_id("sync");

        let mut connect_obs = BaseObserver::with_id("connect", connection_name.clone());
        let connection = local.connect(connection_name.clone()).await?;
        let managed_remote = connection.get_managed_repo()?;
        connect_obs.observe_termination(log::Level::Info, "connected");

        let count = transfer(&local, &local, &managed_remote, connection).await?;

        cleanup_staging(&local).await?;

        let duration = start_time.elapsed();
        let msg = format!(
            "pushed {} files via {} in {duration:.2?}",
            count, connection_name
        );
        sync_obs.observe_termination(log::Level::Info, msg);

        Ok::<(), InternalError>(())
    };

    flightdeck::flightdeck(wrapped, root_builders(), log_path, None, None).await
}

fn root_builders() -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let connect = BaseLayoutBuilderBuilder::default()
        .type_key("connect")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::Static {
            msg: "synchronising...".into(),
            done: "synchronised".into(),
        })
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg}".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let rclone_file = BaseLayoutBuilderBuilder::default()
        .type_key("rclone:file")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdStateFn(Box::new(move |_, id, msg| {
            let id = id.unwrap_or("<missing>".into());
            let msg = msg.unwrap_or("<unknown>".into());
            format!("{msg} {id}")
        })))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg}".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let rclone = BaseLayoutBuilderBuilder::default()
        .type_key("rclone")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                false => msg.unwrap_or("rclone: copying...".into()),
                true => msg.unwrap_or("rclone: done".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({pos}/{len})".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let transfer = BaseLayoutBuilderBuilder::default()
        .type_key("transfer")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, state| match done {
                false => state.map_or("initialising...".into(), |s| match s.as_str() {
                    "download" => "initialising...".into(),
                    "upload" => "initialising...".into(),
                    "preparing" => "selecting files for transfer...".into(),
                    "copying" => "transferring files...".into(),
                    "verifying" => "verifying files...".into(),
                    _ => s,
                }),
                true => "transferred".into(),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg}".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    [
        LayoutItemBuilderNode::from(connect),
        LayoutItemBuilderNode::from(transfer).with_children([LayoutItemBuilderNode::from(rclone)
            .with_children([LayoutItemBuilderNode::from(rclone_file)])]),
    ]
}
