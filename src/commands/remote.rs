use crate::db::models::{Connection, ConnectionType};
use crate::repository::connection::EstablishedConnection;
use crate::repository::local::LocalRepository;
use crate::repository::traits::ConnectionManager;
use crate::utils::errors::InternalError;
use std::path::PathBuf;

pub async fn list(maybe_root: Option<PathBuf>) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root).await?;
    let connections = local_repository.list().await?;

    if connections.is_empty() {
        println!("No connections found.");
    } else {
        let mut table = comfy_table::Table::new();
        table.load_preset(comfy_table::presets::UTF8_FULL);
        table.set_header(vec!["Name", "Connection Type"]);

        for connection in connections {
            table.add_row(vec![
                connection.name,
                match connection.connection_type {
                    ConnectionType::Local => "local".into(),
                    ConnectionType::Ssh => "ssh".into(),
                },
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
    println!("added connection {}", name);

    Ok(())
}
