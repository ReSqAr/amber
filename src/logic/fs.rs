use crate::db::models::{InsertFile, InsertMaterialisation};
use crate::logic::unblobify;
use crate::repository::local::LocalRepository;
use crate::repository::traits::{Adder, Local};
use crate::utils::errors::{AppError, InternalError};
use amber::flightdeck::base::BaseObserver;
use futures::stream;
use std::path::PathBuf;
use tokio::fs;

pub(crate) struct RmResult {
    pub(crate) deleted: usize,
    pub(crate) not_found: usize,
}

pub(crate) async fn rm(
    local: &(impl Adder + Local),
    files: Vec<PathBuf>,
    soft: bool,
) -> Result<RmResult, InternalError> {
    let root_path = local.root().abs().clone().canonicalize()?;

    let mut deleted = 0;
    let mut not_found = 0;
    for file in files {
        let mut obs = BaseObserver::with_id(
            "fs:rm:file",
            file.strip_prefix(root_path.clone())
                .unwrap_or(&*file)
                .to_string_lossy(),
        );

        if fs::metadata(&file)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            let absolute_path = file.canonicalize()?;
            let rel_path = if let Ok(relative) = absolute_path.strip_prefix(&root_path) {
                relative
            } else {
                return Err(
                    AppError::FileNotPartOfRepository(file.to_string_lossy().into()).into(),
                );
            };
            let path = local.root().join(rel_path);

            local
                .add_files(stream::iter([InsertFile {
                    path: rel_path.to_string_lossy().into(),
                    blob_id: None,
                    valid_from: chrono::Utc::now(),
                }]))
                .await?;

            if soft {
                unblobify::unblobify(local, &file).await?;
                obs.observe_termination(log::Level::Info, "deleted [soft]");
            } else {
                fs::remove_file(&path).await?;
                obs.observe_termination(log::Level::Info, "deleted");
            }
            deleted += 1;

            local
                .add_materialisation(stream::iter([InsertMaterialisation {
                    path: rel_path.to_string_lossy().into(),
                    blob_id: None,
                    valid_from: chrono::Utc::now(),
                }]))
                .await?;
        } else {
            not_found += 1;
            obs.observe_termination(log::Level::Warn, "already deleted");
        }
    }

    Ok(RmResult { deleted, not_found })
}

pub(crate) async fn mv(
    local: &LocalRepository,
    source: PathBuf,
    destination: PathBuf,
) -> Result<(), InternalError> {
    let root_path = local.root().abs().clone().canonicalize()?;

    if !fs::metadata(&source)
        .await
        .map(|m| m.is_file())
        .unwrap_or(false)
    {
        return Err(AppError::SourceDoesNotExist(source.to_string_lossy().into()).into());
    }
    if fs::metadata(&destination)
        .await
        .map(|m| m.is_file())
        .unwrap_or(false)
    {
        return Err(AppError::DestinationDoesExist(destination.to_string_lossy().into()).into());
    }

    let mut obs = BaseObserver::with_id(
        "fs:mv:file",
        source
            .strip_prefix(root_path.clone())
            .unwrap_or(&*source)
            .to_string_lossy(),
    );

    let source = source.canonicalize()?;
    let source = if let Ok(relative) = source.strip_prefix(&root_path) {
        relative
    } else {
        return Err(AppError::FileNotPartOfRepository(source.to_string_lossy().into()).into());
    };
    let source = local.root().join(source);

    let destination = std::path::absolute(destination)?;
    let destination = if let Ok(relative) = destination.strip_prefix(&root_path) {
        relative
    } else {
        return Err(AppError::FileNotPartOfRepository(destination.to_string_lossy().into()).into());
    };
    let destination = local.root().join(destination);

    let blob_id = match local.lookup_last_materialisation(&source).await? {
        None => {
            return Err(
                AppError::FileNotPartOfRepository(source.rel().to_string_lossy().into()).into(),
            )
        }
        Some(blob_id) => blob_id,
    };

    let rel_source = source.rel().clone().to_string_lossy().to_string();
    let rel_destination = destination.rel().clone().to_string_lossy().to_string();
    local
        .add_files(stream::iter([
            InsertFile {
                path: rel_source.clone(),
                blob_id: None,
                valid_from: chrono::Utc::now(),
            },
            InsertFile {
                path: rel_destination.clone(),
                blob_id: Some(blob_id.clone()),
                valid_from: chrono::Utc::now(),
            },
        ]))
        .await?;

    fs::rename(&source, &destination).await?;
    obs.observe_termination_ext(
        log::Level::Info,
        "moved",
        [
            ("source".into(), source.abs().to_string_lossy().to_string()),
            (
                "destination".into(),
                destination.abs().to_string_lossy().to_string(),
            ),
        ],
    );

    local
        .add_materialisation(stream::iter([
            InsertMaterialisation {
                path: rel_source,
                blob_id: None,
                valid_from: chrono::Utc::now(),
            },
            InsertMaterialisation {
                path: rel_destination,
                blob_id: Some(blob_id.clone()),
                valid_from: chrono::Utc::now(),
            },
        ]))
        .await?;

    Ok(())
}
