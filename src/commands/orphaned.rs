use crate::db::models::{self};
use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::flightdeck::tracer::Tracer;
use crate::logic::orphaned;
use crate::repository::local::{LocalRepository, LocalRepositoryConfig};
use crate::repository::traits::{Availability, Local, Syncer, VirtualFilesystem};
use crate::utils::errors::InternalError;
use futures::TryStreamExt;

pub(crate) async fn orphaned(
    config: LocalRepositoryConfig,
    output: flightdeck::output::Output,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(config).await?;
    let log_path = local.log_path().abs().clone();

    let wrapped = async {
        list_orphaned_blobs(local.clone()).await?;
        Ok::<(), InternalError>(())
    };

    let wrapped = async {
        let result = wrapped.await;
        local.close().await?;
        result
    };

    flightdeck::flightdeck(wrapped, root_builders(), log_path, None, None, output).await
}

fn root_builders() -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let orphaned = BaseLayoutBuilderBuilder::default()
        .type_key("orphaned::blobs")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("done".into()),
                false => msg.unwrap_or("searching".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} {pos}".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    [LayoutItemBuilderNode::from(orphaned)]
}

pub(crate) async fn list_orphaned_blobs(
    repository: impl Availability
    + Local
    + Syncer<models::File>
    + VirtualFilesystem
    + Sync
    + Send
    + Clone,
) -> Result<(), InternalError> {
    let start_time = tokio::time::Instant::now();
    let mut orphaned_obs = BaseObserver::without_id("orphaned::blobs");

    let tracer = Tracer::new_on("list_orphaned_blobs::orphaned");
    let (close, s) = orphaned::orphaned(repository).await?;
    tracer.measure();

    let tracer = Tracer::new_on("list_orphaned_blobs::stream");
    let mut count: usize = 0;
    let mut s = s;
    while let Some((blob_id, paths)) = s.try_next().await? {
        let path_list = if paths.is_empty() {
            "(unknown path)".to_string()
        } else {
            paths
                .into_iter()
                .map(|p| p.0)
                .collect::<Vec<_>>()
                .join(", ")
        };
        orphaned_obs.observe_state(log::Level::Info, format!("{} -> {path_list}", blob_id.0));
        count += 1;
    }
    tracer.measure();

    let duration = start_time.elapsed();
    let msg = format!("{count} orphaned blob(s) in {duration:.2?}");
    orphaned_obs.observe_termination(log::Level::Info, msg);

    close().await?;

    Ok(())
}
