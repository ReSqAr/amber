use crate::connection::EstablishedConnection;
use crate::db::models::{AvailableBlob, InsertBlob, ObservedBlob};
use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::base::{BaseObservable, BaseObservation};
use crate::flightdeck::observer::Observer;
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::logic::state::VirtualFileState;
use crate::logic::{files, state};
use crate::repository::local::LocalRepository;
use crate::repository::traits::{
    Adder, Availability, BufferType, Config, ConnectionManager, Local, Metadata, RcloneTargetPath,
    VirtualFilesystem,
};
use crate::repository::wrapper::WrappedRepository;
use crate::utils::errors::{AppError, InternalError};
use crate::utils::pipe::TryForwardIntoExt;
use crate::utils::rclone::{run_rclone, Operation, RcloneEvent, RcloneStats, RcloneTarget};
use crate::utils::walker::WalkerConfig;
use crate::utils::{sha256, units};
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tokio_stream::StreamExt;

pub async fn fsck(
    maybe_root: Option<PathBuf>,
    connection_name: Option<String>,
) -> Result<(), InternalError> {
    let local = LocalRepository::new(maybe_root).await?;
    let root_path = local.root().abs().clone();
    let log_path = local.log_path().abs().clone();

    let wrapped = async {
        if let Some(connection_name) = connection_name {
            let connection = local.connect(connection_name.clone()).await?;
            let remote = connection.remote.clone();
            match remote {
                WrappedRepository::Local(_) | WrappedRepository::Grpc(_) => {
                    return Err(InternalError::App(AppError::UnsupportedOperation {
                        connection_name,
                        operation: "fsck".into(),
                    }))
                }

                WrappedRepository::RClone(remote) => {
                    fsck_remote_blobs(&local, &remote, &connection).await?;
                }
            };
        } else {
            fsck_blobs(&local).await?;
            local.reset().await?;
            find_altered_files(local).await?;
        }

        Ok::<(), InternalError>(())
    };

    flightdeck::flightdeck(wrapped, root_builders(&root_path), log_path, None, None).await
}

fn root_builders(root_path: &Path) -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let root = root_path.display().to_string() + "/";

    let root_clone = root.clone();
    let sha = BaseLayoutBuilderBuilder::default()
        .type_key("sha")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdFn(Box::new(move |done, id| {
            let id = id.unwrap_or("<missing>".into());
            let path = id.strip_prefix(root_clone.as_str()).unwrap_or(id.as_str());
            match done {
                true => format!("hashed {}", path),
                false => format!("hashing {}", path),
            }
        })))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} {decimal_bytes}/{decimal_total_bytes}"
                .into(),
            done: "{prefix}✓ {msg} {decimal_bytes}".into(),
        })
        .infallible_build()
        .boxed();

    let fsck_blobs = BaseLayoutBuilderBuilder::default()
        .type_key("fsck:blobs")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("done".into()),
                false => msg.unwrap_or("fsck".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({pos})".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let root_clone = root.clone();
    let file_materialise = BaseLayoutBuilderBuilder::default()
        .type_key("fsck:file:materialise")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdFn(Box::new(move |done, id| {
            let id = id.unwrap_or("<missing>".into());
            let path = id.strip_prefix(root_clone.as_str()).unwrap_or(id.as_str());
            match done {
                true => format!("materialising {}", path),
                false => format!("materialised {}", path),
            }
        })))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg}".into(),
            done: "{prefix}✓ {msg} {decimal_bytes}".into(),
        })
        .infallible_build()
        .boxed();

    let files_materialise = BaseLayoutBuilderBuilder::default()
        .type_key("fsck:files:materialise")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("done".into()),
                false => msg.unwrap_or("materialising known good files".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({pos})".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let rclone = BaseLayoutBuilderBuilder::default()
        .type_key("rclone")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                false => msg.unwrap_or("checking...".into()),
                true => msg.unwrap_or("checked".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} ({pos}/{len})".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let file = BaseLayoutBuilderBuilder::default()
        .type_key("file")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdFn(Box::new(move |done, id| {
            let path = id.unwrap_or("<missing>".into());
            match done {
                true => format!("checked {}", path),
                false => format!("checking {}", path),
            }
        })))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg}".into(),
            done: "{prefix}{spinner:.green} {msg}".into(),
        })
        .infallible_build()
        .boxed();

    let status = BaseLayoutBuilderBuilder::default()
        .type_key("status")
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::StateFn(Box::new(
            |done, msg| match done {
                true => msg.unwrap_or("done".into()),
                false => msg.unwrap_or("checking".into()),
            },
        )))
        .style(Style::Template {
            in_progress: "{prefix}{spinner:.green} {msg} {pos}".into(),
            done: "{prefix}✓ {msg}".into(),
        })
        .infallible_build()
        .boxed();

    [
        LayoutItemBuilderNode::from(fsck_blobs).add_child(sha),
        LayoutItemBuilderNode::from(status).add_child(file),
        LayoutItemBuilderNode::from(files_materialise).add_child(file_materialise),
        LayoutItemBuilderNode::from(rclone),
    ]
}

fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|x| x.as_millis() as i64)
        .unwrap_or(0)
}

async fn fsck_blobs(
    local: &(impl Config + Local + Availability + Adder + Sync + Send + Clone + 'static),
) -> Result<(), InternalError> {
    let start_time = tokio::time::Instant::now();

    let count = Arc::new(Mutex::new(0u64));
    let obs = Arc::new(Mutex::new(BaseObserver::without_id("fsck:blobs")));

    let stream = local.available();

    let local_clone = local.clone();
    let count_clone = count.clone();
    let obs_clone = obs.clone();
    let stream =
        tokio_stream::StreamExt::map(stream, move |blob: Result<AvailableBlob, InternalError>| {
            let local = local_clone.clone();
            let count = count_clone.clone();
            let obs = obs_clone.clone();
            async move {
                let blob = blob?;
                let blob_path = local.blob_path(&blob.blob_id);
                let mut o = BaseObserver::with_id("fsck:blob", blob.blob_id.clone());

                let result = sha256::compute_sha256_and_size(&blob_path).await?;
                let matching = result.hash == blob.blob_id && result.size == blob.blob_size as u64;

                match matching {
                    true => o.observe_termination(log::Level::Debug, "checked"),
                    false => {
                        let quarantine_folder = local.repository_path().join("quarantine");
                        let quarantine_filename =
                            format!("{}.{}", result.hash, current_timestamp());
                        let quarantine_path = quarantine_folder.join(quarantine_filename);
                        o.observe_state_ext(
                            log::Level::Debug,
                            "quarantining blob".to_string(),
                            [(
                                "quarantine_path".into(),
                                quarantine_path.rel().to_string_lossy().into(),
                            )],
                        );

                        fs::create_dir_all(&quarantine_folder).await?;
                        fs::rename(&blob_path, &quarantine_path).await?;
                        o.observe_termination_ext(
                            log::Level::Error,
                            "blob corrupted".to_string(),
                            [(
                                "quarantine_path".into(),
                                quarantine_path.rel().to_string_lossy().into(),
                            )],
                        )
                    }
                };

                *count.lock().await += 1;
                obs.lock()
                    .await
                    .observe_position(log::Level::Trace, *count.lock().await);

                Ok::<InsertBlob, InternalError>(InsertBlob {
                    repo_id: blob.repo_id,
                    blob_id: blob.blob_id,
                    blob_size: blob.blob_size,
                    has_blob: matching,
                    path: None,
                    valid_from: chrono::Utc::now(),
                })
            }
        });

    let stream =
        futures::StreamExt::buffer_unordered(stream, local.buffer_size(BufferType::FsckBuffer));

    stream
        .try_forward_into::<_, _, _, _, InternalError>(|s| async { local.add_blobs(s).await })
        .await?;

    let duration = start_time.elapsed();
    let msg = format!("checked {} blobs in {duration:.2?}", *count.lock().await);
    obs.lock().await.observe_termination(log::Level::Info, msg);

    Ok(())
}

pub async fn find_altered_files(
    local: impl Metadata + Config + Local + Adder + VirtualFilesystem + Clone + Send + Sync + 'static,
) -> Result<(), InternalError> {
    let start_time = tokio::time::Instant::now();
    let mut checker_obs = BaseObserver::without_id("status");

    let (handle, mut stream) = state::state(local, WalkerConfig::default()).await?;

    let mut count: u64 = 0;
    let mut altered_count: u64 = 0;
    while let Some(file_result) = stream.next().await {
        count += 1;
        checker_obs.observe_position(log::Level::Trace, count);

        let file = file_result?;
        if let VirtualFileState::Altered { .. } = file.state {
            altered_count += 1;
            BaseObserver::with_id("file", file.path.clone())
                .observe_termination(log::Level::Error, "altered");
        }
    }

    handle.await??;

    let final_msg = if altered_count > 0 {
        let duration = start_time.elapsed();
        format!("detected {} altered files in {duration:.2?}", altered_count)
    } else {
        "found no altered files".into()
    };
    checker_obs.observe_termination(log::Level::Info, final_msg);

    Ok(())
}

async fn fsck_remote_blobs(
    local: &(impl Config + Local + Adder + Sync + Send + Clone + 'static),
    remote: &(impl Metadata + Availability + RcloneTargetPath),
    connection: &EstablishedConnection,
) -> Result<(), InternalError> {
    let staging_id: u32 = rand::rng().random();
    let fsck_path = local.staging_id_path(staging_id);
    let fsck_files_path = local.rclone_target_path(staging_id);
    let start_time = tokio::time::Instant::now();
    fs::create_dir_all(&fsck_path).await?;
    let rclone_files = fsck_path.join("rclone.files");
    let rclone_source =
        connection.local_rclone_target(fsck_files_path.abs().to_string_lossy().into());
    let rclone_destination = connection.remote_rclone_target(remote.rclone_path(staging_id).await?);

    let expected_count = {
        let (writing_task, tx) =
            write_rclone_files_clone(local, rclone_files.clone().abs().clone());

        let stream = remote.available();
        let local_clone = local.clone();
        let fsck_files_path_clone = fsck_files_path.clone();
        let stream = tokio_stream::StreamExt::map(
            stream,
            move |blob: Result<AvailableBlob, InternalError>| {
                let local = local_clone.clone();
                let fsck_files_path = fsck_files_path_clone.clone();
                let tx = tx.clone();
                async move {
                    let blob = blob?;
                    let blob_path = local.blob_path(&blob.blob_id);
                    let mut o =
                        BaseObserver::with_id("fsck:file:materialise", blob.blob_id.clone());

                    if fs::metadata(&blob_path)
                        .await
                        .map(|m| m.is_file())
                        .unwrap_or(false)
                    {
                        if let Some(path) = blob.path {
                            files::create_hard_link(&blob_path, &fsck_files_path.join(&path))
                                .await?;
                            tx.send(path).await?;
                            o.observe_termination(log::Level::Debug, "materialised");
                        } else {
                            o.observe_termination(log::Level::Debug, "skipped");
                        }
                    } else {
                        o.observe_termination(log::Level::Debug, "skipped");
                    }

                    Ok::<(), InternalError>(())
                }
            },
        );

        let mut stream = futures::StreamExt::buffer_unordered(
            stream,
            local.buffer_size(BufferType::FsckMaterialiseBuffer),
        );

        let mut count = 0u64;
        let mut obs = BaseObserver::without_id("fsck:files:materialise");
        while let Some(result) = stream.next().await {
            count += 1;
            obs.observe_position(log::Level::Trace, count);
            result?;
        }

        drop(stream);
        writing_task.await??;

        let duration = start_time.elapsed();
        let msg = format!("materialised {} blobs for fsck in {duration:.2?}", count);
        obs.observe_termination(log::Level::Info, msg);

        count
    };

    let (tx, rx) = mpsc::unbounded_channel::<RCloneResult>();
    let local_clone = local.clone();
    let repo_id = remote.current().await?.id;
    let result_handler = tokio::spawn(async move {
        let stream = UnboundedReceiverStream::new(rx);
        let stream = stream.map(|r: RCloneResult| match r {
            RCloneResult::Success(object) => {
                BaseObserver::with_id("rclone:check", object.clone())
                    .observe_termination(log::Level::Debug, "OK");
                ObservedBlob {
                    repo_id: repo_id.clone(),
                    has_blob: true,
                    path: object,
                    valid_from: chrono::Utc::now(),
                }
            }
            RCloneResult::Failure(object) => {
                BaseObserver::with_id("rclone:check", object.clone())
                    .observe_termination(log::Level::Error, "altered");
                ObservedBlob {
                    repo_id: repo_id.clone(),
                    has_blob: false,
                    path: object,
                    valid_from: chrono::Utc::now(),
                }
            }
        });
        local_clone.observe_blobs(stream).await?;

        Ok::<(), InternalError>(())
    });

    execute_rclone(
        &fsck_path.abs().clone(),
        rclone_source,
        rclone_destination,
        rclone_files.abs(),
        expected_count,
        tx,
    )
    .await?;

    result_handler.await??;

    Ok(())
}

fn write_rclone_files_clone(
    local: &impl Config,
    rclone_files: PathBuf,
) -> (JoinHandle<Result<(), InternalError>>, mpsc::Sender<String>) {
    let channel_buffer_size = local.buffer_size(BufferType::FsckRcloneFilesWriter);
    let (tx, rx) = mpsc::channel::<String>(channel_buffer_size);

    let writer_buffer_size = local.buffer_size(BufferType::FsckRcloneFilesStream);
    let writing_task = tokio::spawn(async move {
        let file = File::create(rclone_files)
            .await
            .map_err(InternalError::IO)?;
        let mut writer = BufWriter::new(file);

        let mut chunked_stream =
            futures::StreamExt::ready_chunks(ReceiverStream::new(rx), writer_buffer_size);
        while let Some(chunk) = chunked_stream.next().await {
            let data: String = chunk.into_iter().fold(String::new(), |mut acc, path| {
                acc.push_str(&(path + "\n"));
                acc
            });

            writer
                .write_all(data.as_bytes())
                .await
                .map_err(InternalError::IO)?;
        }
        writer.flush().await.map_err(InternalError::IO)?;
        Ok::<(), InternalError>(())
    });

    (writing_task, tx)
}

enum RCloneResult {
    Success(String),
    Failure(String),
}
async fn execute_rclone(
    temp_path: &Path,
    source: RcloneTarget,
    destination: RcloneTarget,
    rclone_files_path: &Path,
    expected_count: u64,
    listener: mpsc::UnboundedSender<RCloneResult>,
) -> Result<u64, InternalError> {
    let count = Arc::new(AtomicU64::new(0));
    let failed_count = Arc::new(AtomicU64::new(0));
    let start_time = tokio::time::Instant::now();
    let mut obs = Observer::without_id("rclone");
    obs.observe_length(log::Level::Trace, expected_count);
    let mut detail_obs = Observer::without_id("rclone:detail");

    let count_clone = Arc::clone(&count);
    let failed_count_clone = Arc::clone(&failed_count);
    let mut files: HashMap<String, Observer<BaseObservable>> = HashMap::new();
    let callback = move |event: RcloneEvent| {
        match event {
            RcloneEvent::Ok(object) => {
                let new_count = count_clone.fetch_add(1, Ordering::Relaxed) + 1;
                obs.observe_position(log::Level::Trace, new_count);
                listener.send(RCloneResult::Success(object)).unwrap();
            }
            RcloneEvent::Fail(object) => {
                let new_count = count_clone.fetch_add(1, Ordering::Relaxed) + 1;
                obs.observe_position(log::Level::Trace, new_count);
                failed_count_clone.fetch_add(1, Ordering::Relaxed);
                listener.send(RCloneResult::Failure(object)).unwrap();
            }
            RcloneEvent::UnknownMessage(msg) => {
                detail_obs.observe_state(log::Level::Debug, msg);
            }
            RcloneEvent::Error(err) => {
                obs.observe_state(log::Level::Error, err);
            }
            RcloneEvent::Copied(_) => {}
            RcloneEvent::Stats(RcloneStats {
                bytes,
                eta,
                total_bytes,
                transferring: checking,
                ..
            }) => {
                let mut seen_files = HashSet::new();

                for item in checking {
                    seen_files.insert(item.name.clone());
                    let f = files.entry(item.name.clone()).or_insert_with(|| {
                        Observer::with_auto_termination(
                            BaseObservable::with_id("rclone:file", item.name.clone()),
                            log::Level::Debug,
                            BaseObservation::TerminalState("done".into()),
                        )
                    });
                    f.observe_state(
                        log::Level::Debug,
                        format!(
                            "{}/{}",
                            units::human_readable_size(item.bytes),
                            units::human_readable_size(item.size),
                        ),
                    );
                }

                files.retain(|key, _| seen_files.contains(key));

                let msg = format!(
                    "{}/{} ETA: {}",
                    units::human_readable_size(bytes),
                    units::human_readable_size(total_bytes),
                    match eta {
                        None => "-".into(),
                        Some(eta) => format!("{}s", eta),
                    }
                );
                obs.observe_state(log::Level::Debug, msg);
            }
            RcloneEvent::UnknownDebugMessage(_) => {}
        };
    };

    let result = run_rclone(
        Operation::Check,
        temp_path,
        rclone_files_path,
        source,
        destination,
        callback,
    )
    .await;

    let final_failed_count = failed_count.load(Ordering::Relaxed);
    let final_count = count.load(Ordering::Relaxed);
    let duration = start_time.elapsed();

    let msg = if final_failed_count > 0 {
        format!("detected {final_failed_count} altered files in {duration:.2?}")
    } else {
        format!("verified {final_count} files in {duration:.2?}")
    };

    if let Err(InternalError::RClone(code)) = result {
        if code != 1 {
            result?;
        } else {
            Observer::without_id("rclone").observe_state(
                log::Level::Debug,
                "rclone with exit code 1 - the errors have been passed through already",
            );
        }
    } else {
        result?;
    }
    Observer::without_id("rclone").observe_termination(log::Level::Info, msg);

    Ok(final_count)
}
