use crate::db::models::{FilePathWithBlobId, InsertMaterialisation};
use crate::flightdeck::base::BaseObserver;
use crate::repository::logic::files;
use crate::repository::traits::{Adder, BufferType, Config, Local, Reconciler};
use crate::utils::errors::InternalError;
use futures::StreamExt;
use tokio::fs;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub async fn materialise(
    local: &(impl Local + Adder + Config + Reconciler + Clone + Send + Sync + 'static),
) -> Result<(), InternalError> {
    let mut count = 0;
    let start_time = tokio::time::Instant::now();
    let mut materialise_obs = BaseObserver::without_id("materialise");

    enum Action {
        Materialised,
        Skipped,
    }

    let (mat_tx, mat_rx) =
        mpsc::channel(local.buffer_size(BufferType::AddFilesDBAddMaterialisations));
    let db_mat_handle = {
        let local_repository = local.clone();
        tokio::spawn(async move {
            local_repository
                .add_materialisation(ReceiverStream::new(mat_rx))
                .await
        })
    };

    let desired_state = local.target_filesystem_state();
    let mat_tx_clone = mat_tx.clone();
    let mut x = desired_state
        .map(|item| {
            let mat_tx = mat_tx_clone.clone();
            async move {
                let FilePathWithBlobId { path, blob_id } = item?;
                let object_path = local.blob_path(blob_id.clone());
                let target_path = local.root().join(path.clone());
                let mut o = BaseObserver::with_id("materialise:file", path.clone());

                if !fs::metadata(&target_path)
                    .await
                    .map(|m| m.is_file())
                    .unwrap_or(false)
                {
                    files::create_hard_link(&object_path, &target_path).await?;

                    let mat = InsertMaterialisation {
                        path: path.clone(),
                        blob_id: blob_id.clone(),
                        valid_from: chrono::Utc::now(),
                    };
                    mat_tx.send(mat).await?;

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

    drop(mat_tx);
    db_mat_handle.await??;

    Ok(())
}
