use crate::db::models::{AvailableBlob, Blob};
use crate::flightdeck;
use crate::flightdeck::base::{
    BaseLayoutBuilderBuilder, BaseObserver, StateTransformer, Style, TerminationAction,
};
use crate::flightdeck::pipes::progress_bars::LayoutItemBuilderNode;
use crate::repository::local::LocalRepository;
use crate::repository::traits::{
    Availability, BufferType, Config, Local, Syncer, VirtualFilesystem,
};
use crate::utils::errors::InternalError;
use crate::utils::pipe::TryForwardIntoExt;
use crate::utils::sha256;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::Mutex;
use tokio::time::Instant;
use uuid::Uuid;

pub async fn fsck(maybe_root: Option<PathBuf>) -> Result<(), InternalError> {
    let local_repository = LocalRepository::new(maybe_root).await?;
    let root_path = local_repository.root().abs().clone();
    let log_path = local_repository.log_path().abs().clone();

    let wrapped = async {
        fsck_blobs(&local_repository).await?;

        local_repository.reset().await?;

        Ok::<(), InternalError>(())
    };

    flightdeck::flightdeck(wrapped, root_builders(&root_path), log_path, None, None).await
}

fn root_builders(root_path: &Path) -> impl IntoIterator<Item = LayoutItemBuilderNode> {
    let root = root_path.display().to_string() + "/";

    let file = BaseLayoutBuilderBuilder::default()
        .type_key("sha")
        .limit(5)
        .termination_action(TerminationAction::Remove)
        .state_transformer(StateTransformer::IdFn(Box::new(move |done, id| {
            let id = id.unwrap_or("<missing>".into());
            let path = id.strip_prefix(root.as_str()).unwrap_or(id.as_str());
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

    [LayoutItemBuilderNode::from(fsck_blobs).add_child(file)]
}

fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|x| x.as_millis() as i64)
        .unwrap_or(0)
}

async fn fsck_blobs(
    local: &(impl Config + Local + Availability + Syncer<Blob> + Sync + Send + Clone + 'static),
) -> Result<(), InternalError> {
    let start_time = Instant::now();

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

                Ok::<Blob, InternalError>(Blob {
                    uuid: Uuid::new_v4().to_string(),
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
        .try_forward_into::<_, _, _, _, InternalError>(|s| async { local.merge(s).await })
        .await?;

    let duration = start_time.elapsed();
    let msg = format!("checked {} blobs in {duration:.2?}", *count.lock().await);
    obs.lock().await.observe_termination(log::Level::Info, msg);

    Ok(())
}
