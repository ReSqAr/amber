use crate::db::models::{Blob, File, Repository, RepositoryName};
use crate::flightdeck::base::BaseObserver;
use crate::flightdeck::observation::Value;
use crate::flightdeck::tracer::Tracer;
use crate::repository::traits::{LastIndicesSyncer, Metadata, Syncer, SyncerParams};
use crate::utils::errors::InternalError;
use crate::utils::pipe::TryForwardIntoExt;
use chrono::SecondsFormat;
use futures::{StreamExt, try_join};
use log::debug;
use tokio::time::Instant;

pub async fn sync_table<I, L, R>(
    local: &L,
    local_param: I::Params,
    remote: &R,
    remote_param: I::Params,
) -> Result<(), InternalError>
where
    I: Send + Sync + 'static + SyncerParams,
    L: Syncer<I> + Send + Sync,
    R: Syncer<I> + Send + Sync,
{
    let local_stream = local.select(local_param).await;
    let remote_stream = remote.select(remote_param).await;

    try_join!(
        async {
            local_stream
                .try_forward_into::<_, _, _, _, InternalError>(|s| remote.merge(s.boxed()))
                .await?;
            debug!("remote: merged type: {}", std::any::type_name::<I>());
            Ok::<_, InternalError>(())
        },
        async {
            remote_stream
                .try_forward_into::<_, _, _, _, InternalError>(|s| local.merge(s.boxed()))
                .await?;
            debug!("local: merged type: {}", std::any::type_name::<I>());
            Ok::<_, InternalError>(())
        },
    )?;

    Ok(())
}

pub async fn sync_repositories<S, T>(local: &S, remote: &T) -> Result<(), InternalError>
where
    S: Metadata
        + LastIndicesSyncer
        + Syncer<Repository>
        + Syncer<File>
        + Syncer<Blob>
        + Syncer<RepositoryName>
        + Clone
        + Send
        + Sync,
    T: Metadata
        + LastIndicesSyncer
        + Syncer<Repository>
        + Syncer<File>
        + Syncer<Blob>
        + Syncer<RepositoryName>
        + Clone
        + Send
        + Sync,
{
    let tracer = Tracer::new_on("sync_repositories");

    let (local_last_indices, remote_last_indices) = try_join!(
        async {
            let tracer = Tracer::new_on("sync_repositories::local_last_indices");
            let local_meta = local.current().await?;
            let result = remote.lookup(local_meta.id).await;
            tracer.measure();
            result
        },
        async {
            let tracer = Tracer::new_on("sync_repositories::remote_last_indices");
            let remote_meta = local.current().await?;
            let result = local.lookup(remote_meta.id).await;
            tracer.measure();
            result
        },
    )?;

    let blobs_then_files = async {
        {
            let mut o = BaseObserver::with_id("sync::table", "blobs");
            let start = Instant::now();
            let created_at = chrono::Utc::now();

            sync_table::<Blob, _, _>(
                local,
                local_last_indices.blob,
                remote,
                remote_last_indices.blob,
            )
            .await?;

            let elapsed = start.elapsed();
            o.observe_termination_ext(
                log::Level::Info,
                "synchronised",
                [
                    ("delay_ns".into(), Value::U64(elapsed.as_nanos() as u64)),
                    (
                        "created_at".into(),
                        Value::String(
                            created_at
                                .to_rfc3339_opts(SecondsFormat::Millis, true)
                                .to_string(),
                        ),
                    ),
                ],
            );
        }

        {
            let mut o = BaseObserver::with_id("sync::table", "files");
            let start = Instant::now();
            let created_at = chrono::Utc::now();

            sync_table::<File, _, _>(
                local,
                local_last_indices.file,
                remote,
                remote_last_indices.file,
            )
            .await?;

            let elapsed = start.elapsed();
            o.observe_termination_ext(
                log::Level::Info,
                "synchronised",
                [
                    ("delay_ns".into(), Value::U64(elapsed.as_nanos() as u64)),
                    (
                        "created_at".into(),
                        Value::String(
                            created_at
                                .to_rfc3339_opts(SecondsFormat::Millis, true)
                                .to_string(),
                        ),
                    ),
                ],
            );
        }

        Ok::<_, InternalError>(())
    };

    let repository_names = async {
        let mut o = BaseObserver::with_id("sync::table", "repository names");
        let start = Instant::now();
        let created_at = chrono::Utc::now();

        sync_table::<RepositoryName, _, _>(
            local,
            local_last_indices.name,
            remote,
            remote_last_indices.name,
        )
        .await?;

        let elapsed = start.elapsed();
        o.observe_termination_ext(
            log::Level::Info,
            "synchronised",
            [
                ("delay_ns".into(), Value::U64(elapsed.as_nanos() as u64)),
                (
                    "created_at".into(),
                    Value::String(
                        created_at
                            .to_rfc3339_opts(SecondsFormat::Millis, true)
                            .to_string(),
                    ),
                ),
            ],
        );
        Ok(())
    };

    try_join!(blobs_then_files, repository_names)?;

    {
        let mut o = BaseObserver::with_id("sync::table", "repositories");
        let start = Instant::now();
        let created_at = chrono::Utc::now();
        try_join!(remote.refresh(), local.refresh())?;
        o.observe_state(log::Level::Info, "prepared");

        sync_table::<Repository, _, _>(local, (), remote, ()).await?;

        let elapsed = start.elapsed();
        o.observe_termination_ext(
            log::Level::Info,
            "synchronised",
            [
                ("delay_ns".into(), Value::U64(elapsed.as_nanos() as u64)),
                (
                    "created_at".into(),
                    Value::String(
                        created_at
                            .to_rfc3339_opts(SecondsFormat::Millis, true)
                            .to_string(),
                    ),
                ),
            ],
        );
    };

    tracer.measure();
    Ok(())
}
