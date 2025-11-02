use crate::db::models::ConnectionName;
use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::logic::{fsck_local, fsck_remote};
use crate::repository::local::{LocalRepository, LocalRepositoryConfig};
use crate::repository::traits::{ConnectionManager, Local};
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::{AppError, InternalError};
use crate::utils::rclone::RCloneConfig;

pub async fn fsck(
    config: LocalRepositoryConfig,
    connection_name: Option<String>,
    output: flightdeck::output::Output,
    rclone_checkers: usize,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(config).await?;
    let log_path = local.log_path().abs().clone();

    let config = RCloneConfig {
        transfers: None,
        checkers: Some(rclone_checkers),
    };

    let wrapped = async {
        if let Some(connection_name) = connection_name {
            let connection = local
                .connect(ConnectionName(connection_name.clone()))
                .await?;
            let remote = connection.remote.clone();
            match remote {
                WrappedRepository::Local(_) | WrappedRepository::Grpc(_) => {
                    return Err(InternalError::App(AppError::UnsupportedOperation {
                        connection_name,
                        operation: "fsck".into(),
                    }));
                }

                WrappedRepository::RClone(remote) => {
                    fsck_remote::fsck_remote(&local, &remote, &connection, config).await?;
                }
            };
        } else {
            fsck_local::fsck_local(&local).await?;
        }

        Ok::<(), InternalError>(())
    };

    flightdeck::flightdeck(wrapped, root_builders(), log_path, None, None, output).await
}

fn root_builders() -> impl IntoIterator<Item = LayoutItemBuilderNode> + use<> {
    let sha = BaseLayoutBuilderBuilder::default()
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

    let fsck_blobs = BaseLayoutBuilderBuilder::default()
        .type_key("fsck:blobs")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("done".into()),
                false => msg.unwrap_or("checking blobs...".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({pos})".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let file_materialise = BaseLayoutBuilderBuilder::default()
        .type_key("fsck:file:materialise")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdFn(Box::new(move |done, id| {
            let id = id.unwrap_or("<missing>".into());
            match done {
                true => format!("materialising {}", id),
                false => format!("materialised {}", id),
            }
        })))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg}".into(),
            done: "{prefix}✓ {msg} {decimal_bytes}".into(),
        })
        .infallible_build()
        .boxed();

    let files_materialise = BaseLayoutBuilderBuilder::default()
        .type_key("fsck:files:materialise")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("done".into()),
                false => msg.unwrap_or("materialising known good files".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({pos})".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let rclone = BaseLayoutBuilderBuilder::default()
        .type_key("rclone")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                false => msg.unwrap_or("checking...".into()),
                true => msg.unwrap_or("checked".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} ETA: {eta} ({pos}/{len})".into(),
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
                false => msg.unwrap_or("checking files...".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({pos})".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let vfs_refresh = BaseLayoutBuilderBuilder::default()
        .type_key("vfs:refresh")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::Static {
            msg: "refreshing virtual file system...".into(),
            done: "refreshed".into(),
        })
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({elapsed})".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    [
        LayoutItemBuilderNode::from(fsck_blobs).add_child(sha),
        LayoutItemBuilderNode::from(vfs_refresh),
        LayoutItemBuilderNode::from(status).with_children([LayoutItemBuilderNode::from(file)]),
        LayoutItemBuilderNode::from(files_materialise).add_child(file_materialise),
        LayoutItemBuilderNode::from(rclone),
    ]
}
