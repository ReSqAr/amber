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
    let local_repository = LocalRepository::new(config).await?;

    let meta = local_repository.current().await?;

    local_repository
        .add_repository_names(
            stream::iter([models::InsertRepositoryName {
                repo_id: meta.id,
                name: name.clone(),
                valid_from: chrono::Utc::now(),
            }])
            .boxed(),
        )
        .await?;

    output.println(format!("renamed repository to {}", name));

    local_repository.close().await?;
    Ok(())
}
