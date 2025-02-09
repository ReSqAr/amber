use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::repository::local::LocalRepository;
use crate::repository::logic::materialise;
use crate::repository::logic::transfer::{cleanup_staging, transfer};
use crate::repository::traits::{ConnectionManager, Local};
use crate::utils::errors::InternalError;
use std::path::PathBuf;

pub async fn pull(
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

        let count = transfer(&local, &managed_remote, &local, connection).await?;

        materialise::materialise(&local).await?;

        cleanup_staging(&local).await?;

        let duration = start_time.elapsed();
        let msg = format!(
            "pulled {} files via {} in {duration:.2?}",
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

    let prep = BaseLayoutBuilderBuilder::default()
        .type_key("transfer:preparation")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::Static {
            msg: "preparing files...".into(),
            done: "files prepared".into(),
        })
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({pos})".into(),
            done: "{prefix}✓ {msg} ({pos})".into(),
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
                false => msg.unwrap_or("copying...".into()),
                true => msg.unwrap_or("copied".into()),
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

    let assimilate = BaseLayoutBuilderBuilder::default()
        .type_key("assimilate")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("verifying files".into()),
                false => msg.unwrap_or("verified".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({pos})".into(),
            done: "{prefix}{pos} {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let materialise_file = BaseLayoutBuilderBuilder::default()
        .type_key("materialise:file")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdStateFn(Box::new(
            move |done, id, _| match done {
                false => format!("materialising {}", id.unwrap_or("<missing>".into())),
                true => format!("materialised {}", id.unwrap_or("<missing>".into())),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg}".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let materialise = BaseLayoutBuilderBuilder::default()
        .type_key("materialise")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("materialised files".into()),
                false => msg.unwrap_or("checking".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} materialising files ({pos})".into(),
            done: "{prefix}{pos} files materialised".into(),
        })
        .infallible_build()
        .boxed();

    [
        LayoutItemBuilderNode::from(connect),
        LayoutItemBuilderNode::from(transfer).with_children([
            LayoutItemBuilderNode::from(prep),
            LayoutItemBuilderNode::from(rclone)
                .with_children([LayoutItemBuilderNode::from(rclone_file)]),
        ]),
        LayoutItemBuilderNode::from(assimilate),
        LayoutItemBuilderNode::from(materialise).add_child(materialise_file),
    ]
}
