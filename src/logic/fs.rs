use crate::db::models;
use crate::db::models::{InsertFile, InsertMaterialisation, Path};
use crate::db::stores::kv;
use crate::db::stores::kv::{AlwaysUpsert, UpsertedValue};
use crate::flightdeck::base::BaseObserver;
use crate::logic::{files, unblobify};
use crate::repository::local::LocalRepository;
use crate::repository::traits::{Adder, BufferType, Config, Local, VirtualFilesystem};
use crate::utils::blake3;
use crate::utils::errors::{AppError, InternalError};
use crate::utils::path::RepoPath;
use crate::utils::pipe::TryForwardIntoExt;
use chrono::Utc;
use futures::{StreamExt, TryStreamExt, stream};
use rand::Rng;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs;
use tokio::sync::Mutex;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum PathType {
    File,
    Dir,
    Unknown,
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

    let mut paths_with_hint: Vec<(RepoPath, String)> = Vec::new();
    for p in paths {
        let (src_is_file, src_is_dir) = fs::metadata(&p)
            .await
            .map(|m| (m.is_file(), m.is_dir()))
            .unwrap_or((false, false));
        let normalised_path = if src_is_file {
            p.to_string_lossy().to_string()
        } else if src_is_dir || p.to_string_lossy().ends_with('/') {
            format!("{}/", p.to_string_lossy().to_string().trim_end_matches('/'))
        } else {
            p.to_string_lossy().to_string()
        };
        let path = RepoPath::from_current(p, &root)?;
        paths_with_hint.push((path, normalised_path));
    }

    let obs_clone = obs.clone();
    let virtual_count_clone = virtual_count.clone();
    let materialised_count_clone = materialised_count.clone();
    let error_count_clone = error_count.clone();

    let transfer_id: u32 = rand::rng().random();
    let transfer_path = local.staging_id_path(transfer_id);
    fs::create_dir_all(&transfer_path)
        .await
        .inspect_err(|e| log::error!("mv: create_dir_all failed: {e}"))?;
    let scratch = kv::Store::<models::Path, models::BlobID>::new(
        transfer_path.join("scratch.rocksdb").abs().to_owned(),
        "scratch".to_string(),
    )
    .await?;

    for (_, normalised_path) in paths_with_hint {
        let s = local
            .select_current_files_with_prefix(normalised_path.clone())
            .await
            .map_err(InternalError::DBError)
            .boxed();
        let s = s.map_ok(|(p, b)| AlwaysUpsert(p, b)).boxed();
        let (s, bg) = scratch.streaming_upsert(s);
        let count = s
            .try_forward_into::<_, _, _, _, InternalError>(|s| async {
                Ok::<_, InternalError>(s.count().await)
            })
            .await?;
        if count == 0 {
            error_count.fetch_add(1, Ordering::Relaxed);
            obs.lock().await.observe_state_ext(
                log::Level::Error,
                "no files match selector",
                [("detail".into(), normalised_path)],
            );
        }
        bg.await??;
    }

    if error_count.load(Ordering::Relaxed) == 0 {
        let now = Utc::now();
        let local = local.clone();
        scratch
            .stream()
            .map_ok(move |(p, _)| InsertFile {
                path: p.clone(),
                blob_id: None,
                valid_from: now,
            })
            .try_forward_into::<_, _, _, _, InternalError>(|s| async {
                local.add_files(s.boxed()).await
            })
            .await
            .inspect_err(|e| log::error!("fs::rm: db failed: {e}"))?;

        let local_clone = local.clone();
        scratch
            .stream()
            .map({
                move |e| {
                    let root = root.clone();
                    let obs = obs_clone.clone();
                    let virtual_count = virtual_count_clone.clone();
                    let materialised_count = materialised_count_clone.clone();
                    let error_count = error_count_clone.clone();

                    {
                        let local = local_clone.clone();
                        async move {
                            let (path, blob_id) = e?;
                            let count = virtual_count.fetch_add(1, Ordering::Relaxed) + 1;
                            obs.lock().await.observe_position(log::Level::Trace, count);
                            let mut o = BaseObserver::with_id("fs:rm:file", &path.0);

                            let p = root.join(&path.0);
                            let metadata = fs::metadata(&p).await.ok();
                            let is_file = metadata.as_ref().is_some_and(|m| m.is_file());

                            if is_file {
                                if hard {
                                    let hash = blake3::compute_blake3_and_size(&p).await?;
                                    if hash.hash == blob_id {
                                        if let Err(e) = fs::remove_file(&p).await {
                                            error_count.fetch_add(1, Ordering::Relaxed);
                                            o.observe_termination(
                                                log::Level::Error,
                                                format!("remove failed: {e}"),
                                            );
                                            return Ok::<_, InternalError>(stream::empty().boxed());
                                        }
                                        o.observe_termination(log::Level::Info, "removed");
                                        materialised_count.fetch_add(1, Ordering::Relaxed);
                                    } else {
                                        o.observe_termination(
                                            log::Level::Warn,
                                            "kept modified file",
                                        );
                                    }
                                } else {
                                    unblobify::unblobify(&local, &p).await?;
                                    o.observe_termination(log::Level::Info, "removed [soft]");
                                }

                                Ok(stream::iter([Ok::<_, InternalError>(InsertMaterialisation {
                                    path,
                                    blob_id: None,
                                })])
                                .boxed())
                            } else {
                                // DB-only removal; nothing to do on disk
                                o.observe_termination(
                                    log::Level::Info,
                                    "removed (not materialised)",
                                );
                                Ok(stream::empty().boxed())
                            }
                        }
                    }
                }
            })
            .buffer_unordered(local.buffer_size(BufferType::FsRmParallelism))
            .try_flatten()
            .try_forward_into::<_, _, _, _, InternalError>(|s| async {
                local.add_materialisation(s.boxed()).await
            })
            .await
            .inspect_err(|e| log::error!("fs::rm: failed: {e}"))?;
    }

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

    scratch.close().await?;
    if error_count > 0 {
        Err(AppError::RmErrors.into())
    } else {
        Ok(())
    }
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

    let (source, destination, path_type) =
        compute_mv_type_hint(local.root(), source, destination).await?;

    let root = local.root();
    let obs_clone = obs.clone();
    let virtual_count_clone = virtual_count.clone();
    let materialised_count_clone = materialised_count.clone();
    let error_count_clone = error_count.clone();

    let transfer_id: u32 = rand::rng().random();
    let transfer_path = local.staging_id_path(transfer_id);
    fs::create_dir_all(&transfer_path)
        .await
        .inspect_err(|e| log::error!("mv: create_dir_all failed: {e}"))?;
    let scratch = kv::Store::<models::Path, models::BlobID>::new(
        transfer_path.join("scratch.rocksdb").abs().to_owned(),
        "scratch".to_string(),
    )
    .await?;

    let source_prefix = source
        .rel()
        .to_string_lossy()
        .to_string()
        .trim_end_matches("/")
        .to_string();
    let destination_prefix = destination
        .rel()
        .to_string_lossy()
        .to_string()
        .trim_end_matches("/")
        .to_string();
    let (source_prefix, destination_prefix) = if path_type == PathType::Dir {
        (
            format!("{source_prefix}/"),
            format!("{destination_prefix}/"),
        )
    } else {
        (source_prefix, destination_prefix)
    };

    let source_prefix_clone = source_prefix.clone();
    let dst = move |src: &Path| match src.0.strip_prefix(&source_prefix_clone) {
        Some(s) => Path(format!("{}{}", destination_prefix, s)),
        None => src.clone(),
    };

    let s = local
        .select_current_files_with_prefix(source_prefix)
        .await
        .map_err(InternalError::DBError);

    let s = s.map_ok(|(p, b)| AlwaysUpsert(p, b)).boxed();
    let (s, bg) = scratch.streaming_upsert(s.boxed());
    let s = s.map_ok(
        |UpsertedValue {
             upsert: AlwaysUpsert(p, b),
             ..
         }| (p, b),
    );
    let s = s.map_ok(|(src, b)| (src.clone(), b));
    let dst_clone = dst.clone();
    let s = local.left_join_current_files(s.boxed(), move |(src, _)| dst_clone(&src));
    let s = s
        .then(async |e| match e {
            Ok(((src, _), cf)) => {
                let violation = if cf.is_some() {
                    Some(("destination already exists", dst(&src)))
                } else if src == dst(&src) {
                    Some(("source equals destination", src))
                } else {
                    None
                };
                if let Some((code, src)) = violation {
                    error_count.fetch_add(1, Ordering::Relaxed);
                    obs.lock().await.observe_state_ext(
                        log::Level::Error,
                        code,
                        [("detail".into(), src.0)],
                    );
                }
                Ok(())
            }
            Err(e) => Err(e),
        })
        .boxed();

    let count = s
        .try_forward_into::<_, _, _, _, InternalError>(|s| async {
            Ok::<_, InternalError>(s.count().await)
        })
        .await?;
    if count == 0 {
        error_count.fetch_add(1, Ordering::Relaxed);
        obs.lock()
            .await
            .observe_state(log::Level::Error, "no files match selector");
    }
    bg.await??;

    if error_count.load(Ordering::Relaxed) == 0 {
        let dst_clone = dst.clone();
        let now = Utc::now();
        scratch
            .stream()
            .map_ok(move |(src, b)| {
                stream::iter::<[Result<_, InternalError>; _]>([
                    Ok(InsertFile {
                        path: src.clone(),
                        blob_id: None,
                        valid_from: now,
                    }),
                    Ok(InsertFile {
                        path: dst_clone(&src),
                        blob_id: Some(b),
                        valid_from: now,
                    }),
                ])
                .boxed()
            })
            .try_flatten()
            .try_forward_into::<_, _, _, _, InternalError>(|s| async {
                local.add_files(s.boxed()).await
            })
            .await
            .inspect_err(|e| log::error!("fs::mv: db failed: {e}"))?;

        scratch
            .stream()
            .map({
                move |e| {
                    let root = root.clone();
                    let obs = obs_clone.clone();
                    let virtual_count = virtual_count_clone.clone();
                    let materialised_count = materialised_count_clone.clone();
                    let error_count = error_count_clone.clone();
                    let dst = dst.clone();
                    async move {
                        let (src, blob_id) = e?;
                        let dst = dst(&src);
                        let mut o = BaseObserver::with_id("fs:mv:file", &src.0);

                        let src = root.join(&src.0);
                        let dst = root.join(&dst.0);

                        let dst_is_file = fs::metadata(&dst)
                            .await
                            .map(|m| m.is_file())
                            .unwrap_or(false);
                        if dst_is_file {
                            error_count.fetch_add(1, Ordering::Relaxed);
                            o.observe_termination(
                                log::Level::Error,
                                format!(
                                    "destination {} already exists",
                                    dst.rel().to_string_lossy()
                                ),
                            );

                            return Ok::<_, InternalError>(stream::empty().boxed());
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
                                        format!(
                                            "moved to {} (materialised)",
                                            dst.rel().to_string_lossy()
                                        ),
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
                                Ok::<_, InternalError>(InsertMaterialisation {
                                    path: models::Path(src.rel().to_string_lossy().into()),
                                    blob_id: None,
                                }),
                                Ok(InsertMaterialisation {
                                    path: models::Path(dst.rel().to_string_lossy().into()),
                                    blob_id: Some(blob_id),
                                }),
                            ])
                            .boxed())
                        } else {
                            o.observe_termination(log::Level::Info, "moved (not materialised)");
                            Ok(stream::empty().boxed())
                        }
                    }
                }
            })
            .buffer_unordered(local.buffer_size(BufferType::FsMvParallelism))
            .try_flatten()
            .try_forward_into::<_, _, _, _, InternalError>(|s| async {
                local.add_materialisation(s.boxed()).await
            })
            .await
            .inspect_err(|e| log::error!("fs::mv: failed: {e}"))?;
    }

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

    files::cleanup_staging(&local.staging_path()).await?;

    let msg = msg.join(" and ");
    let duration = start_time.elapsed();
    let msg = format!("{msg} in {duration:.2?}");

    scratch.close().await?;
    obs.lock().await.observe_termination(log::Level::Info, msg);

    if error_count > 0 {
        Err(AppError::MvErrors.into())
    } else {
        Ok(())
    }
}
