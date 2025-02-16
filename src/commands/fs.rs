use crate::db::models::{InsertFile, InsertMaterialisation};
use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::logic::unblobify;
use crate::repository::local::LocalRepository;
use crate::repository::traits::{Adder, Local};
use crate::utils::errors::{AppError, InternalError};
use futures::stream;
use std::path::{Path, PathBuf};
use tokio::fs;

pub(crate) async fn rm(
    maybe_root: Option<PathBuf>,
    files: Vec<PathBuf>,
    soft: bool,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(maybe_root).await?;
    let root_path = local.root().abs().clone();
    let log_path = local.log_path().abs().clone();

    let wrapped = async {
        let mut obs = BaseObserver::without_id("fs:rm");

        let result = rm_files(&local, files, soft).await?;

        let msg = format!("deleted {} files", result.deleted);
        let msg = if result.not_found > 0 {
            format!(
                "skipped {} already deleted files, {}",
                result.not_found, msg
            )
        } else {
            msg
        };
        obs.observe_termination(log::Level::Info, msg);
        Ok(())
    };

    flightdeck::flightdeck(wrapped, rm_root_builders(&root_path), log_path, None, None).await
}

fn rm_root_builders(root_path: &Path) -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let root = root_path.display().to_string() + "/";

    let file = BaseLayoutBuilderBuilder::default()
        .type_key("fs:rm:file")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdFn(Box::new(move |done, id| {
            let id = id.unwrap_or("<missing>".into());
            let path = id.strip_prefix(root.as_str()).unwrap_or(id.as_str());
            match done {
                true => format!("hashed {}", path),
                false => format!("hashing {}", path),
            }
        })))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg}".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    [LayoutItemBuilderNode::from(file)]
}

struct RmResult {
    deleted: usize,
    not_found: usize,
}

async fn rm_files(
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
    maybe_root: Option<PathBuf>,
    source: PathBuf,
    destination: PathBuf,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(maybe_root).await?;
    let log_path = local.log_path().abs().clone();

    let wrapped = async {
        let mut obs = BaseObserver::without_id("fs:mv");

        mv_file(&local, source.clone(), destination.clone()).await?;

        let msg = format!(
            "moved {} to {}",
            source.to_string_lossy(),
            destination.to_string_lossy()
        );
        obs.observe_termination(log::Level::Info, msg);
        Ok(())
    };

    flightdeck::flightdeck(wrapped, [], log_path, None, None).await
}

async fn mv_file(
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
