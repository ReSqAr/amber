use crate::db::models::{AvailableBlob, InsertBlob};
use crate::flightdeck::base::BaseObserver;
use crate::logic::state;
use crate::logic::state::VirtualFileState;
use crate::repository::traits::{
    Adder, Availability, BufferType, Config, Local, Metadata, VirtualFilesystem,
};
use crate::utils::blake3;
use crate::utils::errors::InternalError;
use crate::utils::pipe::TryForwardIntoExt;
use crate::utils::walker::WalkerConfig;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

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

    let count = Arc::new(AtomicU64::new(0));
    let obs = Arc::new(Mutex::new(BaseObserver::without_id("fsck:blobs")));

    let stream = local.available();

    let local_clone = local.clone();
    let count_clone = count.clone();
    let obs_clone = obs.clone();
    let stream = StreamExt::map(stream, move |blob: Result<AvailableBlob, InternalError>| {
        let local = local_clone.clone();
        let count = count_clone.clone();
        let obs = obs_clone.clone();
        async move {
            let blob = blob?;
            let blob_path = local.blob_path(&blob.blob_id);
            let mut o = BaseObserver::with_id("fsck:blob", blob.blob_id.0.clone());

            // A blob the database believes is here but whose backing file is
            // gone is exactly what fsck exists to find, so it is recorded as
            // lost rather than taking the whole run down with an I/O error.
            let hashed = match blake3::compute_blake3_and_size(&blob_path).await {
                Ok(result) => Some(result),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => return Err(e.into()),
            };

            let matching = hashed
                .as_ref()
                .is_some_and(|r| r.hash == blob.blob_id && r.size == blob.blob_size);

            match hashed {
                None => o.observe_termination(log::Level::Error, "blob missing"),
                Some(_) if matching => o.observe_termination(log::Level::Debug, "checked"),
                Some(result) => {
                    let quarantine_folder = local.repository_path().join("quarantine");
                    let quarantine_filename = format!("{}.{}", result.hash.0, current_timestamp());
                    let quarantine_path = quarantine_folder.join(quarantine_filename);
                    o.observe_state_ext(
                        log::Level::Debug,
                        "quarantining blob".to_string(),
                        [(
                            "quarantine_path".into(),
                            quarantine_path.rel().to_string_lossy().to_string(),
                        )],
                    );

                    fs::create_dir_all(&quarantine_folder).await?;
                    fs::rename(&blob_path, &quarantine_path).await?;
                    o.observe_termination_ext(
                        log::Level::Error,
                        "blob corrupted".to_string(),
                        [(
                            "quarantine_path".into(),
                            quarantine_path.rel().to_string_lossy().to_string(),
                        )],
                    )
                }
            };

            let new_count = count.fetch_add(1, Ordering::Relaxed) + 1;
            obs.lock()
                .await
                .observe_position(log::Level::Trace, new_count);

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

    let stream = futures::StreamExt::buffer_unordered(
        stream,
        local.buffer_size(BufferType::FsckBufferParallelism),
    );

    stream
        .try_forward_into::<_, _, _, _, InternalError>(|s| async {
            local.add_blobs(futures::StreamExt::boxed(s)).await
        })
        .await?;

    let duration = start_time.elapsed();
    let msg = format!(
        "checked {} blobs in {duration:.2?}",
        count.load(Ordering::Relaxed)
    );
    obs.lock().await.observe_termination(log::Level::Info, msg);

    Ok(())
}

async fn find_altered_files(
    local: &(impl Metadata + Config + Local + Adder + VirtualFilesystem + Clone + Send + Sync + 'static),
) -> Result<(), InternalError> {
    let start_time = tokio::time::Instant::now();
    let mut checker_obs = BaseObserver::without_id("status");

    let (handle, mut stream) = state::state(local.clone(), WalkerConfig::default()).await?;

    let mut count: u64 = 0;
    let mut altered_count: u64 = 0;
    let mut incomplete_count: u64 = 0;
    while let Some(file_result) = stream.next().await {
        count += 1;
        checker_obs.observe_position(log::Level::Trace, count);

        let file = file_result?;
        BaseObserver::with_id("file", file.path.0.clone()).observe_termination(
            log::Level::Debug,
            match file.state {
                VirtualFileState::New => "new",
                VirtualFileState::Missing { .. } => "missing",
                VirtualFileState::Ok { .. } => "ok",
                VirtualFileState::OkMaterialisationMissing { .. } => {
                    incomplete_count += 1;
                    "incomplete"
                }
                VirtualFileState::OkBlobMissing { .. } => {
                    incomplete_count += 1;
                    "incomplete"
                }
                VirtualFileState::Altered { .. } => {
                    altered_count += 1;
                    "altered"
                }
                VirtualFileState::Outdated { .. } => "outdated",
            },
        );
    }

    handle.await??;

    let final_msg = if altered_count > 0 || incomplete_count > 0 {
        let duration = start_time.elapsed();
        format!(
            "detected {altered_count} altered files and {incomplete_count} incomplete files in {duration:.2?}"
        )
    } else {
        "found no altered and no incomplete files".into()
    };
    checker_obs.observe_termination(log::Level::Info, final_msg);

    Ok(())
}

pub(crate) async fn fsck_local(
    local: &(
         impl Metadata
         + Config
         + Local
         + Adder
         + Availability
         + VirtualFilesystem
         + Clone
         + Send
         + Sync
         + 'static
     ),
) -> Result<(), InternalError> {
    fsck_blobs(local).await?;

    find_altered_files(local).await?;

    Ok(())
}
