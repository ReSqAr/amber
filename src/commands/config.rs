use crate::db::models;
use crate::repository::local::{LocalRepository, LocalRepositoryConfig};
use crate::repository::traits::{Adder, Metadata};
use crate::utils::errors::InternalError;
use futures::{StreamExt, stream};

pub async fn set_name(
    config: LocalRepositoryConfig,
    name: String,
    output: crate::flightdeck::output::Output,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(config).await?;

    let meta = local.current().await?;

    local
        .add_repository_metadata(
            stream::iter([models::InsertRepositoryMetadata {
                repo_id: meta.id,
                name: Some(name.clone()),
                valid_from: chrono::Utc::now(),
            }])
            .boxed(),
        )
        .await?;

    output.println(format!("renamed repository to {}", name));

    local.close().await?;
    Ok(())
}
