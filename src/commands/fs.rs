use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::logic::fs;
use crate::repository::local::{LocalRepository, LocalRepositoryConfig};
use crate::repository::traits::Local;
use crate::utils::errors::InternalError;
use std::path::{Path, PathBuf};

pub(crate) async fn rm(
    config: LocalRepositoryConfig,
    paths: Vec<PathBuf>,
    hard: bool,
    output: flightdeck::output::Output,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(config).await?;
    let root_path = local.root().abs().clone();
    let log_path = local.log_path().abs().clone();

    let wrapped = async { fs::rm(&local, paths, hard).await };

    let wrapped = async {
        let result = wrapped.await;
        local.close().await?;
        result
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
    config: LocalRepositoryConfig,
    source: PathBuf,
    destination: PathBuf,
    output: flightdeck::output::Output,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(config).await?;
    let log_path = local.log_path().abs().clone();

    let wrapped = async { fs::mv(&local, &source, &destination).await };

    let wrapped = async {
        let result = wrapped.await;
        local.close().await?;
        result
    };

    flightdeck::flightdeck(wrapped, [], log_path, None, None, output).await
}
