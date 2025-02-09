use crate::db::models::FilePathWithBlobId;
use crate::flightdeck::base::BaseObserver;
use crate::repository::traits::{BufferType, Config, Local, Reconciler};
use crate::utils::errors::InternalError;
use futures::StreamExt;
use tokio::fs;

pub async fn checkout(
    local: &(impl Local + Config + Reconciler + Send),
) -> anyhow::Result<(), InternalError> {
    let mut count = 0;
    let start_time = tokio::time::Instant::now();
    let mut checkout_obs = BaseObserver::without_id("checkout");

    enum Action {
        CheckedOut,
        Skipped,
    }

    let desired_state = local.target_filesystem_state();
    let mut x = desired_state
        .map(|item| async move {
            let FilePathWithBlobId { path, blob_id } = item?;
            let object_path = local.blob_path(blob_id.clone());
            let target_path = local.root().join(path.clone());
            let mut o = BaseObserver::with_id("checkout:file", path);

            if !fs::metadata(&target_path)
                .await
                .map(|m| m.is_file())
                .unwrap_or(false)
            {
                if let Some(parent) = target_path.abs().parent() {
                    fs::create_dir_all(parent).await?;
                }
                fs::hard_link(&object_path, &target_path).await?;

                o.observe_termination_ext(
                    log::Level::Info,
                    "hardlinked",
                    [("blob_id".into(), blob_id.clone())],
                );
                Ok::<Action, InternalError>(Action::CheckedOut)
            } else {
                o.observe_termination_ext(
                    log::Level::Trace,
                    "skipped",
                    [("blob_id".into(), blob_id.clone())],
                );
                Ok::<Action, InternalError>(Action::Skipped)
            }
        })
        .buffer_unordered(local.buffer_size(BufferType::Checkout));

    while let Some(next) = x.next().await {
        let action = next?;
        count += match action {
            Action::CheckedOut => 1,
            Action::Skipped => 0,
        };
        checkout_obs.observe_position(log::Level::Trace, count);
    }

    let duration = start_time.elapsed();
    let msg = if count > 0 {
        format!("checked out {count} files in {duration:.2?}")
    } else {
        "no new files checked out".into()
    };
    checkout_obs.observe_termination(log::Level::Info, msg);

    Ok(())
}
