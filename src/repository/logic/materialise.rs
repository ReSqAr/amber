use crate::db::models::{InsertMaterialisation, VirtualFileState};
use crate::flightdeck::base::BaseObserver;
use crate::repository::logic::state::VirtualFile;
use crate::repository::logic::{files, state};
use crate::repository::traits::{Adder, BufferType, Config, Local, Metadata, VirtualFilesystem};
use crate::utils::errors::InternalError;
use crate::utils::walker::WalkerConfig;
use futures::pin_mut;
use tokio::fs;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub async fn materialise(
    local: &(impl Metadata + Local + Adder + VirtualFilesystem + Config + Clone + Send + Sync + 'static),
) -> Result<(), InternalError> {
    let mut count = 0;
    let start_time = tokio::time::Instant::now();
    let mut materialise_obs = BaseObserver::without_id("materialise");

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

    {
        let (state_handle, stream) = state::state(local.clone(), WalkerConfig::default()).await?;

        let stream = futures::TryStreamExt::try_filter(stream, |file_result| {
            let state = file_result.state.clone();
            async move {
                match state {
                    VirtualFileState::New => false,
                    VirtualFileState::Ok => false,
                    VirtualFileState::Missing => true,
                    VirtualFileState::Altered => false,
                    VirtualFileState::Outdated => true,
                    VirtualFileState::NeedsCheck => false,
                }
            }
        });

        let mat_tx = mat_tx.clone();
        let stream = tokio_stream::StreamExt::map(
            stream,
            |file_result: Result<VirtualFile, InternalError>| {
                let mat_tx = mat_tx.clone();
                async move {
                    let VirtualFile {
                        path,
                        target_blob_id,
                        ..
                    } = file_result?;
                    let target_blob_id = target_blob_id.unwrap(); // TODO: shouldn't be needed
                    let object_path = local.blob_path(target_blob_id.clone());
                    let target_path = local.root().join(path.clone());
                    let mut o = BaseObserver::with_id("materialise:file", path.clone());

                    if fs::metadata(&target_path)
                        .await
                        .map(|m| m.is_file())
                        .unwrap_or(false)
                    {
                        files::forced_atomic_hard_link(
                            local,
                            &object_path,
                            &target_path,
                            &target_blob_id,
                        )
                        .await?;
                    } else {
                        files::create_hard_link(&object_path, &target_path).await?;
                    }

                    let mat = InsertMaterialisation {
                        path: path.clone(),
                        blob_id: target_blob_id.clone(),
                        valid_from: chrono::Utc::now(),
                    };
                    mat_tx.send(mat).await?;

                    o.observe_termination_ext(
                        log::Level::Info,
                        "materialised",
                        [("blob_id".into(), target_blob_id.clone())],
                    );
                    Ok::<(), InternalError>(())
                }
            },
        );

        // allow multiple blobify operations to run concurrently
        let stream = futures::StreamExt::buffer_unordered(
            stream,
            local.buffer_size(BufferType::Materialise),
        );

        pin_mut!(stream);
        while let Some(next) = tokio_stream::StreamExt::next(&mut stream).await {
            next?;
            count += 1;
            materialise_obs.observe_position(log::Level::Trace, count);
        }

        state_handle.await??;
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
