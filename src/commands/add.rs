use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::logic::add;
use crate::repository::local::LocalRepository;
use crate::repository::traits::Local;
use crate::utils::errors::InternalError;
use std::path::{Path, PathBuf};
pub async fn add(
    maybe_root: Option<PathBuf>,
    skip_deduplication: bool,
    verbose: bool,
) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root).await?;
    let root_path = local_repository.root().abs().clone();
    let log_path = local_repository.log_path().abs().clone();

    let wrapped = async {
        add::add_files(local_repository, skip_deduplication).await?;
        Ok::<(), InternalError>(())
    };

    let terminal = match verbose {
        true => Some(log::LevelFilter::Debug),
        false => None,
    };
    flightdeck::flightdeck(wrapped, root_builders(&root_path), log_path, None, terminal).await
}

fn root_builders(root_path: &Path) -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let root = root_path.display().to_string() + "/";

    let file = BaseLayoutBuilderBuilder::default()
        .type_key("sha")
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
            in_progress: "{prefix}{spinner:.green} {msg} {decimal_bytes}/{decimal_total_bytes}"
                .into(),
            done: "{prefix}✓ {msg} {decimal_bytes}".into(),
        })
        .infallible_build()
        .boxed();

    let adder = BaseLayoutBuilderBuilder::default()
        .type_key("adder")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("added files".into()),
                false => msg.unwrap_or("adding files".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({pos})".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    [LayoutItemBuilderNode::from(adder).add_child(file)]
}
