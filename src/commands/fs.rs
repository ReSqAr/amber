use crate::db::models::{InsertFile, InsertMaterialisation};
use crate::repository::local::LocalRepository;
use crate::repository::traits::{Adder, Local};
use crate::utils::errors::{AppError, InternalError};
use amber::flightdeck;
use amber::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style, TerminationAction,
};
use amber::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use futures::stream;
use std::path::{Path, PathBuf};
use tokio::fs;

pub(crate) async fn rm(
    maybe_root: Option<PathBuf>,
    files: Vec<PathBuf>,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(maybe_root).await?;
    let root_path = local.root().abs().clone();
    let log_path = local.log_path().abs().clone();

    let wrapped = async {
        let mut obs = BaseObserver::without_id("fs:rm");

        let result = rm_files(&local, files).await?;

        let msg = format!("deleted {} files", result.deleted);
        let msg = if result.not_found > 0 {
            format!("skipped {} already deleted files, {}", result.not_found, msg)  
        } else { 
            msg
        };
        obs.observe_termination(log::Level::Info, msg);
        Ok(())
    };

    flightdeck::flightdeck(wrapped, root_builders(&root_path), log_path, None, None).await
}

fn root_builders(root_path: &Path) -> impl IntoIterator<Item = LayoutItemBuilderNode> {
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

            fs::remove_file(&path).await?;
            deleted += 1;
            obs.observe_termination(log::Level::Info, "deleted");

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

    Ok(RmResult {
        deleted,
        not_found,
    })
}

pub(crate) async fn mv(
    maybe_root: Option<PathBuf>,
    source: PathBuf,
    destination: PathBuf,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(maybe_root).await?;
    todo!()
}
