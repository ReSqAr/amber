use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::logic::add;
use crate::repository::local::LocalRepository;
use crate::repository::traits::Local;
use crate::utils::errors::InternalError;
use std::path::PathBuf;
pub async fn add(
    maybe_root: Option<PathBuf>,
    app_folder: PathBuf,
    verbose: bool,
    output: flightdeck::output::Output,
) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root, app_folder).await?;
    let log_path = local_repository.log_path().abs().clone();

    let wrapped = async {
        add::add_files(local_repository).await?;
        Ok::<(), InternalError>(())
    };

    let terminal = match verbose {
        true => Some(log::LevelFilter::Debug),
        false => None,
    };
    flightdeck::flightdeck(wrapped, root_builders(), log_path, None, terminal, output).await
}

fn root_builders() -> impl IntoIterator<Item = LayoutItemBuilderNode> + use<> {
    let file = BaseLayoutBuilderBuilder::default()
        .type_key("sha")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdFn(Box::new(move |done, id| {
            let id = id.unwrap_or("<missing>".into());
            match done {
                true => format!("hashed {}", id),
                false => format!("hashing {}", id),
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

    let vfs_refresh = BaseLayoutBuilderBuilder::default()
        .type_key("vfs:refresh")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::Static {
            msg: "refreshing virtual file system...".into(),
            done: "refreshed".into(),
        })
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({elapsed})".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let scanner = BaseLayoutBuilderBuilder::default()
        .type_key("scanner")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("scanned files".into()),
                false => msg.unwrap_or("scanning files".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({pos})".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    [
        LayoutItemBuilderNode::from(vfs_refresh),
        LayoutItemBuilderNode::from(scanner)
            .add_child(LayoutItemBuilderNode::from(adder).add_child(file)),
    ]
}
