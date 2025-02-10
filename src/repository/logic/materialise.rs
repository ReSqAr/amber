use crate::db::models::FilePathWithBlobId;
use crate::flightdeck::base::BaseObserver;
use crate::repository::logic::files;
use crate::repository::traits::{BufferType, Config, Local, Reconciler};
use crate::utils::errors::InternalError;
use futures::StreamExt;
use tokio::fs;

pub async fn materialise(
    local: &(impl Local + Config + Reconciler + Send),
) -> Result<(), InternalError> {
    let mut count = 0;
    let start_time = tokio::time::Instant::now();
    let mut materialise_obs = BaseObserver::without_id("materialise");

    enum Action {
        Materialised,
        Skipped,
    }

    let desired_state = local.target_filesystem_state();
    let mut x = desired_state
        .map(|item| async move {
            let FilePathWithBlobId { path, blob_id } = item?;
            let object_path = local.blob_path(blob_id.clone());
            let target_path = local.root().join(path.clone());
            let mut o = BaseObserver::with_id("materialise:file", path);

            if !fs::metadata(&target_path)
                .await
                .map(|m| m.is_file())
                .unwrap_or(false)
            {
                files::create_hard_link(&object_path, &target_path).await?;

                o.observe_termination_ext(
                    log::Level::Info,
                    "materialised",
                    [("blob_id".into(), blob_id.clone())],
                );
                Ok::<Action, InternalError>(Action::Materialised)
            } else {
                o.observe_termination_ext(
                    log::Level::Trace,
                    "skipped",
                    [("blob_id".into(), blob_id.clone())],
                );
                Ok::<Action, InternalError>(Action::Skipped)
            }
        })
        .buffer_unordered(local.buffer_size(BufferType::Materialise));

    while let Some(next) = x.next().await {
        let action = next?;
        count += match action {
            Action::Materialised => 1,
            Action::Skipped => 0,
        };
        materialise_obs.observe_position(log::Level::Trace, count);
    }

    let duration = start_time.elapsed();
    let msg = if count > 0 {
        format!("materialised {count} files in {duration:.2?}")
    } else {
        "no new files materialised".into()
    };
    materialise_obs.observe_termination(log::Level::Info, msg);

    Ok(())
}
