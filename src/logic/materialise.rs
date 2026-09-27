use crate::db::models;
use crate::db::models::{BlobID, InsertMaterialisation};
use crate::flightdeck;
use crate::flightdeck::base::BaseObserver;
use crate::flightdeck::tracer::Tracer;
use crate::logic::state::VirtualFileState;
use crate::logic::{files, state};
use crate::repository::traits::{Adder, BufferType, Config, Local, Metadata, VirtualFilesystem};
use crate::utils::errors::InternalError;
use crate::utils::path::RepoPath;
use crate::utils::walker::WalkerConfig;
use futures::{StreamExt, pin_mut};
use tokio::{fs, task};

/// What the walker found at a path.
enum Observed {
    File,
    Nothing,
}

/// A path that is taken on disk, although the walker found no file there.
struct Clash;

/// The other name under which the directory holds the entry `path` resolves
/// to - `photo.jpg` for `Photo.jpg` on a filesystem that ignores case.
#[cfg(unix)]
async fn occupant(path: &RepoPath) -> Option<String> {
    use std::os::unix::fs::MetadataExt;

    let path = path.abs().clone();
    task::spawn_blocking(move || {
        let wanted = std::fs::symlink_metadata(&path).ok()?;
        let own_name = path.file_name()?;
        std::fs::read_dir(path.parent()?)
            .ok()?
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name() != own_name)
            .find(|entry| {
                entry
                    .metadata()
                    .is_ok_and(|m| m.dev() == wanted.dev() && m.ino() == wanted.ino())
            })
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
    })
    .await
    .ok()
    .flatten()
}

#[cfg(not(unix))]
async fn occupant(_path: &RepoPath) -> Option<String> {
    None
}

pub async fn materialise(
    local: &(impl Metadata + Local + Adder + VirtualFilesystem + Config + Clone + Send + Sync + 'static),
) -> Result<(), InternalError> {
    let tracer = Tracer::new_on("materialise");

    let mut materialised_count = 0;
    let mut deleted_count = 0;
    let mut skipped_count = 0;
    let start_time = tokio::time::Instant::now();
    let mut materialise_obs = BaseObserver::without_id("materialise");

    let (mat_tx, mat_rx) = flightdeck::tracked::mpsc_channel(
        "materialise::mat",
        local.buffer_size(BufferType::AddFilesDBAddMaterialisationsChannelSize),
    );
    let db_mat_handle = {
        let local_repository = local.clone();
        tokio::spawn(async move { local_repository.add_materialisation(mat_rx.boxed()).await })
    };

    fs::create_dir_all(&local.staging_path()).await?;

    {
        let (state_handle, stream) = state::state(local.clone(), WalkerConfig::default()).await?;

        struct ToMaterialise {
            path: models::Path,
            target_blob_id: Option<BlobID>,
            observed: Observed,
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
                VirtualFileState::OkMaterialisationMissing { target_blob_id } => {
                    Some(Ok(ToMaterialise {
                        path,
                        observed: Observed::File,
                        target_blob_id: Some(target_blob_id),
                    }))
                }
                VirtualFileState::OkBlobMissing { target_blob_id } => Some(Ok(ToMaterialise {
                    path,
                    observed: Observed::File,
                    target_blob_id: Some(target_blob_id),
                })),
                VirtualFileState::Missing {
                    target_blob_id,
                    local_has_target_blob,
                } => match local_has_target_blob {
                    true => Some(Ok(ToMaterialise {
                        path,
                        observed: Observed::Nothing,
                        target_blob_id: Some(target_blob_id),
                    })),
                    false => {
                        BaseObserver::with_id("materialise:file", path.0)
                            .observe_termination(log::Level::Warn, "unavailable");
                        None
                    }
                },
                VirtualFileState::Altered { .. } => None,
                VirtualFileState::Outdated {
                    target_blob_id,
                    local_has_target_blob,
                    ..
                } => match (local_has_target_blob, target_blob_id) {
                    (true, Some(target_blob_id)) => Some(Ok(ToMaterialise {
                        path,
                        observed: Observed::File,
                        target_blob_id: Some(target_blob_id),
                    })),
                    (_, None) => Some(Ok(ToMaterialise {
                        path,
                        observed: Observed::File,
                        target_blob_id: None,
                    })),
                    (false, Some(_)) => {
                        BaseObserver::with_id("materialise:file", path.0)
                            .observe_termination(log::Level::Warn, "unavailable");
                        None
                    }
                },
            }
        });

        enum Action {
            Materialised,
            Deleted,
            Skipped,
        }

        let mat_tx = mat_tx.clone();
        let stream = tokio_stream::StreamExt::map(
            stream,
            |file_result: Result<ToMaterialise, InternalError>| {
                let mat_tx = mat_tx.clone();
                async move {
                    let ToMaterialise {
                        path,
                        observed,
                        target_blob_id,
                    } = file_result?;
                    let target_path = local.root().join(path.0.clone());
                    let mut o = BaseObserver::with_id("materialise:file", path.0.clone());

                    let action = match target_blob_id.clone() {
                        // Nothing is at the path, as far as the walker saw - yet
                        // something may answer to it: on a filesystem that ignores
                        // case or Unicode normalisation, `Photo.jpg` finds
                        // `photo.jpg`, which another path of the repository may
                        // have put there. Linking would replace that file.
                        Some(target_blob_id) if matches!(observed, Observed::Nothing) => {
                            let object_path = local.blob_path(&target_blob_id);
                            let linked = match fs::symlink_metadata(&target_path).await {
                                Ok(_) => Err(Clash),
                                Err(_) => {
                                    match files::create_link(
                                        &object_path,
                                        &target_path,
                                        local.capability(),
                                    )
                                    .await
                                    {
                                        Ok(()) => Ok(()),
                                        // another path got there first
                                        Err(InternalError::IO(e))
                                            if e.kind() == std::io::ErrorKind::AlreadyExists =>
                                        {
                                            Err(Clash)
                                        }
                                        Err(e) => return Err(e),
                                    }
                                }
                            };
                            if let Err(Clash) = linked {
                                let msg = match occupant(&target_path).await {
                                    Some(name) => format!("skipped: clashes with {name} on disk"),
                                    None => "skipped: the path is taken on disk".into(),
                                };
                                o.observe_termination(log::Level::Warn, msg);
                                return Ok(Action::Skipped);
                            }

                            o.observe_termination_ext(
                                log::Level::Info,
                                "materialised",
                                [("blob_id".into(), target_blob_id.0.clone())],
                            );

                            Action::Materialised
                        }
                        Some(target_blob_id) => {
                            let object_path = local.blob_path(&target_blob_id);
                            if fs::metadata(&target_path)
                                .await
                                .map(|m| m.is_file())
                                .unwrap_or(false)
                            {
                                files::forced_atomic_link(
                                    local,
                                    &object_path,
                                    &target_path,
                                    &target_blob_id,
                                )
                                .await?;
                            } else {
                                files::create_link(&object_path, &target_path, local.capability())
                                    .await?;
                            }

                            o.observe_termination_ext(
                                log::Level::Info,
                                "materialised",
                                [("blob_id".into(), target_blob_id.0.clone())],
                            );

                            Action::Materialised
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

                            Action::Deleted
                        }
                    };

                    let mat = InsertMaterialisation {
                        path: path.clone(),
                        blob_id: target_blob_id,
                    };
                    mat_tx.send(mat).await?;

                    Ok::<Action, InternalError>(action)
                }
            },
        );

        // allow multiple blobify operations to run concurrently
        let stream = futures::StreamExt::buffer_unordered(
            stream,
            local.buffer_size(BufferType::MaterialiseParallelism),
        );

        pin_mut!(stream);
        while let Some(next) = tokio_stream::StreamExt::next(&mut stream).await {
            match next? {
                Action::Materialised => materialised_count += 1,
                Action::Deleted => deleted_count += 1,
                Action::Skipped => skipped_count += 1,
            }
            materialise_obs.observe_position(log::Level::Trace, materialised_count + deleted_count);
        }

        state_handle.await??;
    }

    let mut parts = Vec::new();
    if materialised_count > 0 {
        parts.push(format!("materialised {} files", materialised_count))
    }
    if deleted_count > 0 {
        parts.push(format!("deleted {} files", deleted_count))
    }
    if skipped_count > 0 {
        parts.push(format!(
            "skipped {} files whose path another file already takes",
            skipped_count
        ))
    }

    let msg = if !parts.is_empty() {
        let duration = start_time.elapsed();
        format!("{} in {duration:.2?}", parts.join(" and "))
    } else {
        "no new files materialised".into()
    };
    materialise_obs.observe_termination(log::Level::Info, msg);

    drop(mat_tx);
    db_mat_handle.await??;

    tracer.measure();
    Ok(())
}
