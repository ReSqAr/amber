use crate::db::models::{Connection, ConnectionType};
use crate::repository::connection::EstablishedConnection;
use crate::repository::local::LocalRepository;
use crate::repository::traits::ConnectionManager;
use std::path::PathBuf;

pub async fn list(maybe_root: Option<PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(maybe_root).await?;

    for connection in local_repository.list().await? {
        println!("{} ({})", connection.name, connection.connection_type);
    }

    Ok(())
}
pub async fn add(
    maybe_root: Option<PathBuf>,
    name: String,
    connection_type: ConnectionType,
    parameter: String,
) -> Result<(), Box<dyn std::error::Error>> {
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
