use crate::db::models::{BlobTransferItem, FileTransferItem};
use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::logic::files;
use crate::logic::sync;
use crate::logic::transfer::transfer;
use crate::repository::local::{LocalRepository, LocalRepositoryConfig};
use crate::repository::traits::{ConnectionManager, Local, Metadata};
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::InternalError;
use crate::utils::path::RepoPath;
use crate::utils::rclone::RCloneConfig;
use std::path::PathBuf;

pub async fn push(
    config: LocalRepositoryConfig,
    connection_name: String,
    paths: Vec<PathBuf>,
    output: flightdeck::output::Output,
    rclone_transfers: usize,
    rclone_checkers: usize,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(config).await?;
    let log_path = local.log_path().abs().clone();
    let root = local.root();
    let paths = paths
        .iter()
        .map(|p| RepoPath::from_current(p, &root))
        .collect::<Result<Vec<_>, _>>()?;

    let config = RCloneConfig {
        transfers: Some(rclone_transfers),
        checkers: Some(rclone_checkers),
    };

    let wrapped = async {
        let start_time = tokio::time::Instant::now();
        let mut sync_obs = BaseObserver::without_id("sync");

        let mut connect_obs = BaseObserver::with_id("connect", connection_name.clone());
        let connection = local.connect(connection_name.clone()).await?;
        let remote = connection.remote.clone();
        let remote_meta = remote.current().await?;
        connect_obs.observe_termination(log::Level::Info, "connected");

        let count = match remote {
            WrappedRepository::Local(remote) => {
                sync::sync_repositories(&local, &remote).await?;
                transfer::<BlobTransferItem>(&local, &local, &remote, connection, paths, config)
                    .await?
            }
            WrappedRepository::Grpc(remote) => {
                sync::sync_repositories(&local, &remote).await?;
                transfer::<BlobTransferItem>(&local, &local, &remote, connection, paths, config)
                    .await?
            }
            WrappedRepository::RClone(remote) => {
                transfer::<FileTransferItem>(&local, &local, &remote, connection, paths, config)
                    .await?
            }
        };

        files::cleanup_staging(&local.staging_path()).await?;

        let duration = start_time.elapsed();
        let msg = format!(
            "pushed {} blobs via {} to {} in {duration:.2?}",
            count, connection_name, remote_meta.name
        );
        sync_obs.observe_termination(log::Level::Info, msg);

        Ok::<(), InternalError>(())
    };

    flightdeck::flightdeck(wrapped, root_builders(), log_path, None, None, output).await
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

    let sync_table = BaseLayoutBuilderBuilder::default()
        .type_key("sync:table")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdFn(Box::new(|done, id| match done {
            false => format!(
                "synchronising known {}...",
                id.unwrap_or("<unknown>".into())
            ),
            true => format!("synchronised known {}", id.unwrap_or("<unknown>".into())),
        })))
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
            msg: "preparing blobs...".into(),
            done: "blobs prepared".into(),
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
            let msg = msg.unwrap_or("copying".into());
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
                    "preparing" => "selecting blobs for transfer...".into(),
                    "copying" => "transferring blobs...".into(),
                    "verifying" => "verifying blobs...".into(),
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

    let file = BaseLayoutBuilderBuilder::default()
        .type_key("sha")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdFn(Box::new(move |done, id| {
            let id = id.unwrap_or("<missing>".into());
            match done {
                true => format!("hashed {}", id),
                false => format!("hashing {}", id),
            }
        })))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} {decimal_bytes}/{decimal_total_bytes}"
                .into(),
            done: "{prefix}✓ {msg} {decimal_bytes}".into(),
        })
        .infallible_build()
        .boxed();

    let assimilate = BaseLayoutBuilderBuilder::default()
        .type_key("assimilate")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                false => msg.unwrap_or("assimilating...".into()),
                true => msg.unwrap_or("assimilated".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({pos})".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    [
        LayoutItemBuilderNode::from(connect),
        LayoutItemBuilderNode::from(sync_table),
        LayoutItemBuilderNode::from(transfer).with_children([
            LayoutItemBuilderNode::from(prep),
            LayoutItemBuilderNode::from(rclone)
                .with_children([LayoutItemBuilderNode::from(rclone_file)]),
            LayoutItemBuilderNode::from(assimilate)
                .with_children([LayoutItemBuilderNode::from(file)]),
        ]),
    ]
}
