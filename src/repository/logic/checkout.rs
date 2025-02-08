use crate::db::models::FilePathWithBlobId;
use crate::repository::traits::{Local, Reconciler};
use crate::utils::errors::InternalError;
use amber::flightdeck::base::{BaseObservation, BaseObserver};
use futures::StreamExt;
use std::collections::HashMap;
use tokio::fs;

pub async fn checkout(
    local: &(impl Local + Reconciler + Send),
) -> anyhow::Result<(), InternalError> {
    let mut count = 0;
    let start_time = tokio::time::Instant::now();
    let mut checkout_obs = BaseObserver::without_id("checkout");

    let mut desired_state = local.target_filesystem_state();
    while let Some(next) = desired_state.next().await {
        let FilePathWithBlobId {
            path: relative_path,
            blob_id,
        } = next?;

        let mut o = BaseObserver::with_id("checkout:file", relative_path.clone());

        let object_path = local.blob_path(blob_id.clone());
        let target_path = local.root().join(relative_path);

        if !fs::metadata(&target_path)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            if let Some(parent) = target_path.abs().parent() {
                fs::create_dir_all(parent).await?;
            }
            fs::hard_link(&object_path, &target_path).await?;

            count += 1;
            checkout_obs.observe(log::Level::Trace, BaseObservation::Position(count));
            o.observe(
                log::Level::Info,
                BaseObservation::TerminalStateWithData {
                    state: "hardlinked".into(),
                    data: HashMap::from([("blob_id".into(), blob_id.clone())]),
                },
            );
        } else {
            o.observe(
                log::Level::Trace,
                BaseObservation::TerminalStateWithData {
                    state: "skipped".into(),
                    data: HashMap::from([("blob_id".into(), blob_id.clone())]),
                },
            );
        };
    }

    let duration = start_time.elapsed();
    let msg = if count > 0 {
        format!("checked out {count} files in {duration:.2?}")
    } else {
        "no files checked out".into()
    };
    checkout_obs.observe(log::Level::Info, BaseObservation::TerminalState(msg));

    Ok(())
}
