use crate::db::models::{InsertFile, InsertMaterialisation};
use crate::flightdeck::base::BaseObserver;
use crate::logic::unblobify;
use crate::repository::local::LocalRepository;
use crate::repository::traits::{Adder, Local};
use crate::utils::errors::{AppError, InternalError};
use crate::utils::path::RepoPath;
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
    let root = local.root();

    let mut deleted = 0;
    let mut not_found = 0;
    for file in files {
        let path = RepoPath::from_current(&file, &root)?;
        let mut obs = BaseObserver::with_id("fs:rm:file", path.rel().to_string_lossy());

        if fs::metadata(&file)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            local
                .add_files(stream::iter([InsertFile {
                    path: path.rel().to_string_lossy().to_string(),
                    blob_id: None,
                    valid_from: chrono::Utc::now(),
                }]))
                .await?;

            if soft {
                unblobify::unblobify(local, &path).await?;
                obs.observe_termination(log::Level::Info, "deleted [soft]");
            } else {
                fs::remove_file(&path).await?;
                obs.observe_termination(log::Level::Info, "deleted");
            }
            deleted += 1;

            local
                .add_materialisation(stream::iter([InsertMaterialisation {
                    path: path.rel().to_string_lossy().to_string(),
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
    let root = local.root();

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

    let source = RepoPath::from_current(source, &root)?;
    let destination = RepoPath::from_current(destination, &root)?;

    let mut obs = BaseObserver::with_id("fs:mv:file", source.rel().to_string_lossy());

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
            ("source".into(), rel_source.clone()),
            ("destination".into(), rel_destination.clone()),
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
