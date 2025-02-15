use crate::db::models::InsertMaterialisation;
use crate::flightdeck::base::BaseObserver;
use crate::logic::state::VirtualFileState;
use crate::logic::{files, state};
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

        struct ToMaterialise {
            path: String,
            target_blob_id: Option<String>,
        }

        let stream = futures::StreamExt::filter_map(stream, |file_result| async move {
            let file_result = match file_result {
                Ok(file_result) => file_result,
                Err(e) => return Some(Err(e)),
            };
            let path = file_result.path;
            let state = file_result.state;
            match state {
                VirtualFileState::New => None,
                VirtualFileState::Ok { .. } => None,
                VirtualFileState::Missing {
                    target_blob_id,
                    local_has_target_blob,
                } => match local_has_target_blob {
                    true => Some(Ok(ToMaterialise {
                        path,
                        target_blob_id: Some(target_blob_id),
                    })),
                    false => None,
                },
                VirtualFileState::Altered { .. } => None,
                VirtualFileState::Outdated {
                    target_blob_id,
                    local_has_target_blob,
                    ..
                } => match local_has_target_blob {
                    true => Some(Ok(ToMaterialise {
                        path,
                        target_blob_id,
                    })),
                    false => None,
                },
            }
        });

        let mat_tx = mat_tx.clone();
        let stream = tokio_stream::StreamExt::map(
            stream,
            |file_result: Result<ToMaterialise, InternalError>| {
                let mat_tx = mat_tx.clone();
                async move {
                    let ToMaterialise {
                        path,
                        target_blob_id,
                    } = file_result?;
                    let target_path = local.root().join(path.clone());
                    let mut o = BaseObserver::with_id("materialise:file", path.clone());

                    match target_blob_id.clone() {
                        Some(target_blob_id) => {
                            let object_path = local.blob_path(&target_blob_id);
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

                            o.observe_termination_ext(
                                log::Level::Info,
                                "materialised",
                                [("blob_id".into(), target_blob_id.clone())],
                            );
                        }
                        None => {
                            if fs::metadata(&target_path)
                                .await
                                .map(|m| m.is_file())
                                .unwrap_or(false)
                            {
                                fs::remove_file(&target_path).await?;
                                o.observe_termination(log::Level::Info, "deleted");
                            }
                        }
                    }

                    let mat = InsertMaterialisation {
                        path: path.clone(),
                        blob_id: target_blob_id,
                        valid_from: chrono::Utc::now(),
                    };
                    mat_tx.send(mat).await?;

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
