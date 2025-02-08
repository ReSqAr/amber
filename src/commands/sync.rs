use crate::db::models::{Blob, File, Repository};
use crate::flightdeck;
use crate::flightdeck::base::BaseObserver;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::repository::local::LocalRepository;
use crate::repository::logic::{checkout, sync};
use crate::repository::traits::{ConnectionManager, LastIndicesSyncer, Local, Metadata, Syncer};
use crate::utils::errors::{AppError, InternalError};
use std::path::PathBuf;

pub async fn sync(
    maybe_root: Option<PathBuf>,
    connection_name: Option<String>,
) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root).await?;
    let log_path = local_repository.log_path().abs().clone();

    let wrapped = async {
        let start_time = tokio::time::Instant::now();
        let mut sync_obs = BaseObserver::without_id("sync");

        connect_sync_checkout(local_repository, connection_name.clone()).await?;

        let duration = start_time.elapsed();
        let msg = match connection_name {
            None => format!("synchronised in {duration:.2?}"),
            Some(connection_name) => {
                format!("synchronised via {} in {duration:.2?}", connection_name)
            }
        };
        sync_obs.observe_termination(log::Level::Info, msg);

        Ok::<(), InternalError>(())
    };

    flightdeck::flightdeck(wrapped, root_builders(), log_path, None, None).await
}

fn root_builders() -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let connect = BaseLayoutBuilderBuilder::default()
        .type_key("connect")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdFn(Box::new(|done, id| match done {
            false => format!("connecting via {}...", id.unwrap_or("<unknown>".into())),
            true => format!("connected via {}", id.unwrap_or("<unknown>".into())),
        })))
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

    let checkout_file = BaseLayoutBuilderBuilder::default()
        .type_key("checkout:file")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdStateFn(Box::new(
            move |done, id, _| match done {
                false => format!("checking {}", id.unwrap_or("<missing>".into())),
                true => format!("checked {}", id.unwrap_or("<missing>".into())),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg}".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let checkout = BaseLayoutBuilderBuilder::default()
        .type_key("checkout")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("checked out files".into()),
                false => msg.unwrap_or("checking".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {pos} files checked out".into(),
            done: "{prefix}{pos} files checked out".into(),
        })
        .infallible_build()
        .boxed();

    [
        LayoutItemBuilderNode::from(connect),
        LayoutItemBuilderNode::from(sync_table),
        LayoutItemBuilderNode::from(checkout).add_child(checkout_file),
    ]
}

async fn connect_sync_checkout(
    local: LocalRepository,
    connection_name: Option<String>,
) -> Result<(), InternalError> {
    if let Some(connection_name) = connection_name {
        let mut connect_obs = BaseObserver::with_id("connect", connection_name.clone());
        let connection = local.connect(connection_name.clone()).await?;
        let managed_remote = match connection.remote.as_managed() {
            Some(tracked_remote) => tracked_remote,
            None => {
                return Err(AppError::UnsupportedRemote {
                    connection_name,
                    operation: "sync".into(),
                }
                .into());
            }
        };
        connect_obs.observe_termination(log::Level::Info, "connected");

        sync_repositories(&local, &managed_remote).await?;
    }

    checkout::checkout(&local).await?;

    Ok(())
}

pub async fn sync_repositories<S, T>(local: &S, remote: &T) -> Result<(), InternalError>
where
    S: Metadata
        + LastIndicesSyncer
        + Syncer<Repository>
        + Syncer<File>
        + Syncer<Blob>
        + Clone
        + Send
        + Sync,
    T: Metadata
        + LastIndicesSyncer
        + Syncer<Repository>
        + Syncer<File>
        + Syncer<Blob>
        + Clone
        + Send
        + Sync,
{
    let local_repo_id = local.repo_id().await?;
    let remote_repo_id = remote.repo_id().await?;

    let local_last_indices = remote.lookup(local_repo_id).await?;
    let remote_last_indices = local.lookup(remote_repo_id).await?;

    {
        let mut o = BaseObserver::with_id("sync:table", "files");
        sync::sync_table::<File, _, _>(
            local,
            local_last_indices.file,
            remote,
            remote_last_indices.file,
        )
        .await?;
        o.observe_termination(log::Level::Info, "synchronised");
    }

    {
        let mut o = BaseObserver::with_id("sync:table", "blobs");
        sync::sync_table::<Blob, _, _>(
            local,
            local_last_indices.blob,
            remote,
            remote_last_indices.blob,
        )
        .await?;
        o.observe_termination(log::Level::Info, "synchronised");
    }

    {
        let mut o = BaseObserver::with_id("sync:table", "repositories");
        remote.refresh().await?;
        local.refresh().await?;
        o.observe_state(log::Level::Info, "prepared");

        sync::sync_table::<Repository, _, _>(local, (), remote, ()).await?;
        o.observe_termination(log::Level::Info, "synchronised");
    }

    Ok(())
}
