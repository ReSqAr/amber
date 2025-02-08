use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style,
    TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::repository::local::LocalRepository;
use crate::repository::traits::Local;
use crate::utils::errors::InternalError;
use std::path::PathBuf;

pub async fn init_repository(maybe_root: Option<PathBuf>) -> Result<(), InternalError> {
    let wrapped = async {
        let start_time = tokio::time::Instant::now();
        let mut init_obs = BaseObserver::without_id("init");

        let local = LocalRepository::create(maybe_root).await?;

        let duration = start_time.elapsed();
        let msg = format!(
            "initialised repository {} in {duration:.2?}",
            local.root().abs().display()
        );
        init_obs.observe_termination(log::Level::Info, msg);

        Ok::<(), InternalError>(())
    };

    flightdeck::flightdeck(wrapped, root_builders(), None, None, None).await
}

fn root_builders() -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let init = BaseLayoutBuilderBuilder::default()
        .type_key("init")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("initialised".into()),
                false => msg.unwrap_or("initialising".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg}".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    [LayoutItemBuilderNode::from(init)]
}
