use crate::connection::EstablishedConnection;
use crate::db::models::{Connection, ConnectionName, ConnectionType};
use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::repository::local::{LocalRepository, LocalRepositoryConfig};
use crate::repository::traits::{ConnectionManager, Metadata};
use crate::utils::errors::InternalError;
use crate::utils::rclone;

fn render_connection_type(connection_type: ConnectionType) -> String {
    match connection_type {
        ConnectionType::Local => "local".into(),
        ConnectionType::RClone => "rclone".into(),
        ConnectionType::Ssh => "ssh".into(),
    }
}

pub async fn list(
    config: LocalRepositoryConfig,
    output: flightdeck::output::Output,
) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(config).await?;
    let mut connections = local_repository.list().await?;

    if connections.is_empty() {
        output.println("No connections found.".to_string());
    } else {
        let mut table = comfy_table::Table::new();
        table.load_preset(comfy_table::presets::UTF8_FULL);
        table.set_header(vec!["Name", "Connection Type"]);

        connections.sort_by_key(|c| c.name.clone());

        for connection in connections {
            table.add_row(vec![
                connection.name.0,
                render_connection_type(connection.connection_type),
            ]);
        }

        output.println(format!("{table}"));
    }

    local_repository.close().await?;
    Ok(())
}

pub async fn add(
    config: LocalRepositoryConfig,
    name: String,
    connection_type: ConnectionType,
    parameter: String,
    output: flightdeck::output::Output,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(config).await?;

    let wrapped = async {
        let start_time = tokio::time::Instant::now();
        let mut init_obs = BaseObserver::without_id("init");

        let connection = add_connection(
            ConnectionName(name.clone()),
            connection_type.clone(),
            parameter,
            &local,
        )
        .await?;
        let remote_meta = connection.remote.current().await?;

        let duration = start_time.elapsed();
        let msg = format!(
            "added connection via {} ({}) to {} in {duration:.2?}",
            name,
            render_connection_type(connection_type),
            remote_meta.name,
        );
        init_obs.observe_termination(log::Level::Info, msg);

        connection.close().await?;
        Ok::<(), InternalError>(())
    };

    let result = flightdeck::flightdeck(wrapped, root_builders(), None, None, None, output).await;

    let close_result = local.close().await;
    result.and(close_result)
}

fn root_builders() -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let connect = BaseLayoutBuilderBuilder::default()
        .type_key("connect")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::Static {
            msg: "connecting".into(),
            done: "connected".into(),
        })
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg}".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    [LayoutItemBuilderNode::from(connect)]
}

async fn add_connection(
    name: ConnectionName,
    connection_type: ConnectionType,
    parameter: String,
    local: &LocalRepository,
) -> Result<EstablishedConnection, InternalError> {
    rclone::check_rclone().await?;

    let established = EstablishedConnection::new(
        local.clone(),
        name.clone(),
        connection_type.clone(),
        parameter.clone(),
    )
    .await?;

    let connection = Connection {
        name: name.clone(),
        connection_type,
        parameter,
    };

    local.add(connection).await?;

    Ok(established)
}
