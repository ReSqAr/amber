use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::logic::fs;
use crate::repository::local::LocalRepository;
use crate::repository::traits::Local;
use crate::utils::errors::InternalError;
use std::path::{Path, PathBuf};

pub(crate) async fn rm(
    maybe_root: Option<PathBuf>,
    app_folder: PathBuf,
    files: Vec<PathBuf>,
    soft: bool,
    output: flightdeck::output::Output,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(maybe_root, app_folder).await?;
    let root_path = local.root().abs().clone();
    let log_path = local.log_path().abs().clone();

    let wrapped = async {
        let mut obs = BaseObserver::without_id("fs:rm");

        let result = fs::rm(&local, files, soft).await?;

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

    flightdeck::flightdeck(
        wrapped,
        rm_root_builders(&root_path),
        log_path,
        None,
        None,
        output,
    )
    .await
}

fn rm_root_builders(root_path: &Path) -> impl IntoIterator<Item = LayoutItemBuilderNode> + use<> {
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

pub(crate) async fn mv(
    maybe_root: Option<PathBuf>,
    app_folder: PathBuf,
    source: PathBuf,
    destination: PathBuf,
    output: flightdeck::output::Output,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(maybe_root, app_folder).await?;
    let log_path = local.log_path().abs().clone();

    let wrapped = async {
        let mut obs = BaseObserver::without_id("fs:mv");

        fs::mv(&local, source.clone(), destination.clone()).await?;

        let msg = format!(
            "moved {} to {}",
            source.to_string_lossy(),
            destination.to_string_lossy()
        );
        obs.observe_termination(log::Level::Info, msg);
        Ok(())
    };

    flightdeck::flightdeck(wrapped, [], log_path, None, None, output).await
}
