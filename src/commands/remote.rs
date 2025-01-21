use crate::db::models::{Connection, ConnectionType};
use crate::repository::local_repository::LocalRepository;
use crate::repository::traits::ConnectionManager;

pub async fn list() -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    for connection in local_repository.list().await? {
        println!("{} ({})", connection.name, connection.connection_type);
    }

    Ok(())
}
pub async fn add(
    name: String,
    connection_type: ConnectionType,
    parameter: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let local_repository = LocalRepository::new(None).await?;

    crate::repository::connection::Connection::connect(
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
