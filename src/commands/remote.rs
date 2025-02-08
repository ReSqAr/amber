use crate::db::models::{Connection, ConnectionType};
use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style,
    TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::repository::connection::EstablishedConnection;
use crate::repository::local::LocalRepository;
use crate::repository::traits::ConnectionManager;
use crate::utils::errors::InternalError;
use std::path::PathBuf;

fn render_connection_type(connection_type: ConnectionType) -> String {
    match connection_type {
        ConnectionType::Local => "local".into(),
        ConnectionType::Ssh => "ssh".into(),
    }
}

pub async fn list(maybe_root: Option<PathBuf>) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root).await?;
    let mut connections = local_repository.list().await?;

    if connections.is_empty() {
        println!("No connections found.");
    } else {
        let mut table = comfy_table::Table::new();
        table.load_preset(comfy_table::presets::UTF8_FULL);
        table.set_header(vec!["Name", "Connection Type"]);

        connections.sort_by_key(|c| c.name.clone());

        for connection in connections {
            table.add_row(vec![
                connection.name,
                render_connection_type(connection.connection_type),
            ]);
        }

        println!("{table}");
    }

    Ok(())
}

pub async fn add(
    maybe_root: Option<PathBuf>,
    name: String,
    connection_type: ConnectionType,
    parameter: String,
) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root).await?;

    let wrapped = async {
        let start_time = tokio::time::Instant::now();
        let mut init_obs = BaseObserver::without_id("init");

        add_connection(
            name.clone(),
            connection_type.clone(),
            parameter,
            local_repository,
        )
        .await?;

        let duration = start_time.elapsed();
        let msg = format!(
            "added connection {} ({}) in {duration:.2?}",
            name,
            render_connection_type(connection_type)
        );
        init_obs.observe_termination(log::Level::Info, msg);

        Ok::<(), InternalError>(())
    };

    flightdeck::flightdeck(wrapped, root_builders(), None, None, None).await?;

    Ok(())
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
    name: String,
    connection_type: ConnectionType,
    parameter: String,
    local_repository: LocalRepository,
) -> Result<(), InternalError> {
    EstablishedConnection::connect(
        local_repository.clone(),
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

    local_repository.add(&connection).await?;

    Ok(())
}
