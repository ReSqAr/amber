use crate::db::models;
use crate::repository::local::LocalRepository;
use crate::repository::traits::{Adder, Metadata};
use crate::utils::errors::InternalError;
use futures::stream;
use std::path::PathBuf;

pub async fn set_name(
    maybe_root: Option<PathBuf>,
    app_folder: PathBuf,
    name: String,
    output: crate::flightdeck::output::Output,
) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root, app_folder).await?;

    let meta = local_repository.current().await?;

    local_repository
        .add_repository_names(stream::iter([models::InsertRepositoryName {
            repo_id: meta.id,
            name: name.clone(),
            valid_from: chrono::Utc::now(),
        }]))
        .await?;

    output.println(format!("renamed repository to {}", name));

    Ok(())
}
