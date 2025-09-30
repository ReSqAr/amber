use crate::db::models::{
    InsertMaterialisation, MoveEvent, MoveViolationCode, PathType, RmEvent, RmViolationCode,
};
use crate::flightdeck::base::BaseObserver;
use crate::logic::unblobify;
use crate::repository::local::LocalRepository;
use crate::repository::traits::{Adder, BufferType, Config, Local, VirtualFilesystem};
use crate::utils::errors::{AppError, InternalError};
use crate::utils::path::RepoPath;
use crate::utils::pipe::TryForwardIntoExt;
use chrono::Utc;
use futures::{StreamExt, TryStreamExt, stream};
use futures_core::stream::BoxStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs;
use tokio::sync::Mutex;

pub(crate) async fn rm(
    local: &LocalRepository,
    paths: Vec<PathBuf>,
    hard: bool,
) -> Result<(), InternalError> {
    let start_time = tokio::time::Instant::now();
    let error_count = Arc::new(AtomicU64::new(0));
    let virtual_count = Arc::new(AtomicU64::new(0));
    let materialised_count = Arc::new(AtomicU64::new(0));
    let obs = Arc::new(Mutex::new(BaseObserver::without_id("fs:rm")));

    let root = local.root();

    let mut paths_with_hint: Vec<(String, PathType)> = Vec::new();
    for p in paths {
        let (src_is_file, src_is_dir) = fs::metadata(&p)
            .await
            .map(|m| (m.is_file(), m.is_dir()))
            .unwrap_or((false, false));
        let path_type = if src_is_file {
            PathType::File
        } else if src_is_dir || p.to_string_lossy().ends_with('/') {
            PathType::Dir
        } else {
            PathType::Unknown
        };
        let path = RepoPath::from_current(p, &root)?;
        paths_with_hint.push((path.rel().to_string_lossy().to_string(), path_type));
    }

    let obs_clone = obs.clone();
    let virtual_count_clone = virtual_count.clone();
    let materialised_count_clone = materialised_count.clone();
    let error_count_clone = error_count.clone();

    local
        .remove_files(paths_with_hint, Utc::now())
        .await
        .map({
            move |ev| {
                let root = root.clone();
                let obs = obs_clone.clone();
                let virtual_count = virtual_count_clone.clone();
                let materialised_count = materialised_count_clone.clone();
                let error_count = error_count_clone.clone();

                async move {
                    match ev? {
                        RmEvent::Violation(v) => {
                            error_count.fetch_add(1, Ordering::Relaxed);
                            let code = match v.code {
                                RmViolationCode::SourceNotFound => "source not found",
                            };
                            obs.lock().await.observe_state_ext(
                                log::Level::Error,
                                code,
                                [("detail".into(), v.detail)],
                            );
                            Ok(stream::iter([]).boxed())
                                as Result<
                                    BoxStream<Result<InsertMaterialisation, InternalError>>,
                                    InternalError,
                                >
                        }
                        RmEvent::Instruction(instr) => {
                            let count = virtual_count.fetch_add(1, Ordering::Relaxed) + 1;
                            obs.lock().await.observe_position(log::Level::Trace, count);
                            let mut o = BaseObserver::with_id("fs:rm:file", &instr.path);

                            let p = root.join(&instr.path);
                            let exists_as_file =
                                fs::metadata(&p).await.map(|m| m.is_file()).unwrap_or(false);

                            if exists_as_file {
                                if hard {
                                    if let Err(e) = fs::remove_file(&p).await {
                                        error_count.fetch_add(1, Ordering::Relaxed);
                                        o.observe_termination(
                                            log::Level::Error,
                                            format!("remove failed: {e}"),
                                        );
                                        return Ok(stream::iter([]).boxed());
                                    }
                                    o.observe_termination(log::Level::Info, "removed");
                                } else {
                                    unblobify::unblobify(local, &p).await?;
                                    o.observe_termination(log::Level::Info, "removed [soft]");
                                }
                                materialised_count.fetch_add(1, Ordering::Relaxed);

                                Ok(stream::iter([Ok(InsertMaterialisation {
                                    path: instr.path,
                                    blob_id: None,
                                    valid_from: Utc::now(),
                                })])
                                .boxed())
                            } else {
                                // DB-only removal; nothing to do on disk
                                o.observe_termination(
                                    log::Level::Info,
                                    "removed (not materialised)",
                                );
                                Ok(stream::iter([]).boxed())
                            }
                        }
                    }
                }
            }
        })
        .buffer_unordered(local.buffer_size(BufferType::FsRmParallelism))
        .try_flatten()
        .try_forward_into::<_, _, _, _, InternalError>(|s| async {
            local.add_materialisation(s).await
        })
        .await
        .inspect_err(|e| log::error!("fs::rm: failed: {e}"))?;

    // summary
    let mut msg = vec![];
    let virtual_count = virtual_count.load(Ordering::Relaxed);
    if virtual_count > 0 {
        let materialised_count = materialised_count.load(Ordering::Relaxed);
        msg.push(format!(
            "removed {virtual_count} files ({materialised_count} on disk)"
        ));
    } else {
        msg.push("no files removed".into())
    }
    let error_count = error_count.load(Ordering::Relaxed);
    if error_count > 0 {
        msg.push(format!("encountered {error_count} errors"));
    }
    let msg = msg.join(" and ");
    let duration = start_time.elapsed();
    obs.lock()
        .await
        .observe_termination(log::Level::Info, format!("{msg} in {duration:.2?}"));

    if error_count > 0 {
        Err(AppError::RmErrors.into())
    } else {
        Ok(())
    }
}

pub async fn compute_mv_type_hint(
    root: RepoPath,
    source: &PathBuf,
    destination: &PathBuf,
) -> Result<(RepoPath, RepoPath, PathType), InternalError> {
    let source_ends_with_slash = source.to_string_lossy().ends_with("/");
    let destination_ends_with_slash = destination.to_string_lossy().ends_with("/");

    let src = RepoPath::from_current(source, &root)?;
    let dst = RepoPath::from_current(destination, &root)?;

    let (src_is_file, src_is_dir) = fs::metadata(&src)
        .await
        .map(|m| (m.is_file(), m.is_dir()))
        .unwrap_or((false, false));
    let (dst_is_file, dst_is_dir) = fs::metadata(&dst)
        .await
        .map(|m| (m.is_file(), m.is_dir()))
        .unwrap_or((false, false));

    let src_folder_like = source_ends_with_slash || src_is_dir;
    let dst_folder_like = destination_ends_with_slash || dst_is_dir;

    if dst_is_file {
        return Err(AppError::DestinationDoesExist(source.to_string_lossy().to_string()).into());
    }

    if src_is_file && dst_folder_like {
        return Err(
            AppError::DestinationIsAFolder(destination.to_string_lossy().to_string()).into(),
        );
    }

    if src_is_file && !dst_folder_like {
        return Ok((src, dst, PathType::File));
    }

    if src_folder_like || dst_folder_like {
        return Ok((src, dst, PathType::Dir));
    }

    Ok((src, dst, PathType::Unknown))
}

pub(crate) async fn mv(
    local: &LocalRepository,
    source: &PathBuf,
    destination: &PathBuf,
) -> Result<(), InternalError> {
    let start_time = tokio::time::Instant::now();
    let error_count = Arc::new(AtomicU64::new(0));
    let virtual_count = Arc::new(AtomicU64::new(0));
    let materialised_count = Arc::new(AtomicU64::new(0));
    let obs = Arc::new(Mutex::new(BaseObserver::without_id("fs:mv")));

    let (source, destination, mv_type_hint) =
        compute_mv_type_hint(local.root(), source, destination).await?;

    let root = local.root();
    let obs_clone = obs.clone();
    let virtual_count_clone = virtual_count.clone();
    let materialised_count_clone = materialised_count.clone();
    let error_count_clone = error_count.clone();
    local
        .move_files(
            source.rel().to_string_lossy().into(),
            destination.rel().to_string_lossy().into(),
            mv_type_hint,
            Utc::now(),
        )
        .await
        .map({
            move |ev| {
                let root = root.clone();
                let obs = obs_clone.clone();
                let virtual_count = virtual_count_clone.clone();
                let materialised_count = materialised_count_clone.clone();
                let error_count = error_count_clone.clone();
                async move {
                    match ev? {
                        MoveEvent::Violation(v) => {
                            error_count.fetch_add(1, Ordering::Relaxed);

                            let code = match v.code {
                                MoveViolationCode::SourceNotFound => "found no files to move",
                                MoveViolationCode::DestinationExistsDb => {
                                    "destination already exists"
                                }
                                MoveViolationCode::SourceEqualsDestination => {
                                    "source equals destination"
                                }
                            };
                            obs.lock().await.observe_state_ext(
                                log::Level::Error,
                                code,
                                [("detail".into(), v.detail)],
                            );

                            Ok(stream::iter([]).boxed())
                                as Result<
                                    BoxStream<Result<InsertMaterialisation, InternalError>>,
                                    InternalError,
                                >
                        }
                        MoveEvent::Instruction(instr) => {
                            let mut o = BaseObserver::with_id("fs:mv:file", &instr.src_path);

                            let src = root.join(&instr.src_path);
                            let dst = root.join(&instr.dst_path);

                            let dst_is_file = fs::metadata(&dst)
                                .await
                                .map(|m| m.is_file())
                                .unwrap_or(false);
                            if dst_is_file {
                                error_count.fetch_add(1, Ordering::Relaxed);
                                o.observe_termination(
                                    log::Level::Error,
                                    format!("destination {} already exists", instr.dst_path),
                                );

                                return Ok(stream::iter([]).boxed());
                            }

                            let count = virtual_count.fetch_add(1, Ordering::Relaxed) + 1;
                            obs.lock().await.observe_position(log::Level::Trace, count);

                            let src_is_file = fs::metadata(&src)
                                .await
                                .map(|m| m.is_file())
                                .unwrap_or(false);
                            if src_is_file {
                                if let Some(parent) = dst.abs().parent() {
                                    fs::create_dir_all(parent).await?;
                                }
                                match fs::rename(&src, &dst).await {
                                    Ok(_) => {
                                        materialised_count.fetch_add(1, Ordering::Relaxed);
                                        o.observe_termination(
                                            log::Level::Info,
                                            format!("moved to {} (materialised)", instr.dst_path),
                                        );
                                    }
                                    Err(e) => {
                                        o.observe_termination(
                                            log::Level::Error,
                                            format!("rename failed: {e}"),
                                        );
                                        return Err(e.into());
                                    }
                                }

                                Ok(stream::iter([
                                    Ok(InsertMaterialisation {
                                        path: instr.src_path,
                                        blob_id: None,
                                        valid_from: Utc::now(),
                                    }),
                                    Ok(InsertMaterialisation {
                                        path: instr.dst_path,
                                        blob_id: Some(instr.blob_id),
                                        valid_from: Utc::now(),
                                    }),
                                ])
                                .boxed())
                            } else {
                                o.observe_termination(log::Level::Info, "moved (not materialised)");
                                Ok(stream::iter([]).boxed())
                            }
                        }
                    }
                }
            }
        })
        .buffer_unordered(local.buffer_size(BufferType::FsMvParallelism))
        .try_flatten()
        .try_forward_into::<_, _, _, _, InternalError>(|s| async {
            local.add_materialisation(s).await
        })
        .await
        .inspect_err(|e| log::error!("fs::mv: failed: {e}"))?;

    let mut msg = vec![];

    let virtual_count = virtual_count.load(Ordering::Relaxed);
    if virtual_count > 0 {
        let materialised_count = materialised_count.load(Ordering::Relaxed);
        msg.push(format!(
            "moved {virtual_count} files ({materialised_count} materialised)",
        ));
    } else {
        msg.push("no files moved".into())
    }

    let error_count = error_count.load(Ordering::Relaxed);
    if error_count > 0 {
        msg.push(format!("encountered {error_count} errors"));
    }

    let msg = msg.join(" and ");
    let duration = start_time.elapsed();
    let msg = format!("{msg} in {duration:.2?}");

    obs.lock().await.observe_termination(log::Level::Info, msg);

    if error_count > 0 {
        Err(AppError::MvErrors.into())
    } else {
        Ok(())
    }
}
