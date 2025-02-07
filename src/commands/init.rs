use crate::repository::local::LocalRepository;
use crate::repository::traits::Local;
use crate::utils::errors::InternalError;
use amber::flightdeck;
use amber::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObservable, BaseObservation, StateTransformer, Style,
    TerminationAction,
};
use amber::flightdeck::observer::Observer;
use amber::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use std::path::PathBuf;

pub async fn init_repository(maybe_root: Option<PathBuf>) -> Result<(), InternalError> {
    let wrapped = async {
        let start_time = tokio::time::Instant::now();
        let mut init_obs = Observer::new(BaseObservable::without_id("init"));

        let local = LocalRepository::create(maybe_root).await?;

        let duration = start_time.elapsed();
        let msg = format!(
            "initialised repository {} in {duration:.2?}",
            local.root().abs().display()
        );
        init_obs.observe(log::Level::Info, BaseObservation::TerminalState(msg));

        Ok::<(), InternalError>(())
    };

    flightdeck::flightdeck(wrapped, root_builders(), None, None, None).await?;

    Ok(())
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
