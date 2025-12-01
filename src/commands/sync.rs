use crate::db::models::ConnectionName;
use crate::flightdeck;
use crate::flightdeck::base::BaseObserver;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::logic::{materialise, sync};
use crate::repository::local::{LocalRepository, LocalRepositoryConfig};
use crate::repository::traits::{ConnectionManager, Local};
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::{AppError, InternalError};

pub async fn sync(
    config: LocalRepositoryConfig,
    connection_name: Option<String>,
    output: flightdeck::output::Output,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(config).await?;
    let log_path = local.log_path().abs().clone();

    let wrapped = async {
        let start_time = tokio::time::Instant::now();
        let mut sync_obs = BaseObserver::without_id("sync");

        connect_sync_materialise(local.clone(), connection_name.clone()).await?;

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

    let result =
        flightdeck::flightdeck(wrapped, root_builders(), log_path, None, None, output).await;

    let close_result = local.close().await;
    result.and(close_result)
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

    let materialise_file = BaseLayoutBuilderBuilder::default()
        .type_key("materialise:file")
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
        LayoutItemBuilderNode::from(sync_table),
        LayoutItemBuilderNode::from(materialise).add_child(materialise_file),
    ]
}

async fn connect_sync_materialise(
    local: LocalRepository,
    connection_name: Option<String>,
) -> Result<(), InternalError> {
    if let Some(connection_name) = connection_name {
        let mut connect_obs = BaseObserver::with_id("connect", connection_name.clone());
        let connection = local
            .connect(ConnectionName(connection_name.clone()))
            .await?;
        let remote = connection.remote.clone();
        connect_obs.observe_termination(log::Level::Info, "connected");

        match remote {
            WrappedRepository::Local(remote) => {
                sync::sync_repositories(&local, &remote).await?;
            }
            WrappedRepository::Grpc(remote) => {
                sync::sync_repositories(&local, &remote).await?;
            }
            WrappedRepository::RClone(_) => {
                return Err(InternalError::App(AppError::UnsupportedOperation {
                    connection_name: connection_name.to_string(),
                    operation: "sync".to_string(),
                }));
            }
        };

        connection.close().await?;
    }

    materialise::materialise(&local).await?;

    Ok(())
}
