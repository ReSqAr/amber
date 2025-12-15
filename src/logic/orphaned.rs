use crate::db::models;
use crate::db::stores::kv;
use crate::db::stores::kv::UpsertAction;
use crate::repository::traits::{Availability, Local, Syncer, VirtualFilesystem};
use crate::utils::errors::InternalError;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use rand::Rng;
use std::collections::BTreeSet;

#[derive(Clone)]
struct InsertBlob(models::BlobID);

impl kv::Upsert for InsertBlob {
    type K = models::BlobID;
    type V = BTreeSet<models::Path>;

    fn key(&self) -> Self::K {
        self.0.clone()
    }

    fn upsert(self, existing: Option<Self::V>) -> UpsertAction<Self::V> {
        match existing {
            Some(_v) => UpsertAction::NoChange,
            None => UpsertAction::Change(BTreeSet::new()),
        }
    }
}

#[derive(Clone)]
struct RemoveBlob(models::BlobID);

impl kv::Upsert for RemoveBlob {
    type K = models::BlobID;
    type V = BTreeSet<models::Path>;

    fn key(&self) -> Self::K {
        self.0.clone()
    }

    fn upsert(self, _: Option<Self::V>) -> UpsertAction<Self::V> {
        UpsertAction::Delete
    }
}

#[derive(Clone)]
struct RecordBlobPath(models::BlobID, models::Path);

impl kv::Upsert for RecordBlobPath {
    type K = models::BlobID;
    type V = BTreeSet<models::Path>;

    fn key(&self) -> Self::K {
        self.0.clone()
    }

    fn upsert(self, existing: Option<Self::V>) -> UpsertAction<Self::V> {
        if let Some(mut paths) = existing {
            paths.insert(self.1.clone());
            UpsertAction::Change(paths)
        } else {
            UpsertAction::NoChange
        }
    }
}

pub async fn orhpaned<'a>(
    repository: impl Availability
    + Local
    + Syncer<models::File>
    + VirtualFilesystem
    + Sync
    + Send
    + Clone,
) -> Result<
    (
        Box<dyn FnOnce() -> BoxFuture<'static, Result<(), InternalError>>>,
        BoxStream<'a, Result<(models::BlobID, BTreeSet<models::Path>), InternalError>>,
    ),
    InternalError,
> {
    let staging_id: u32 = rand::rng().random();
    let scratch_path = repository.staging_id_path(staging_id);
    tokio::fs::create_dir_all(&scratch_path).await?;
    let scratch = kv::Store::<models::BlobID, BTreeSet<models::Path>>::new(
        scratch_path.join("scratch.rocksdb").abs().to_owned(),
        "orphaned".to_string(),
    )
    .await?;

    let blob_available = repository.available();
    let blob_stream = blob_available.map(|r| r.map(|blob| InsertBlob(blob.blob_id)));
    scratch.upsert(blob_stream.boxed()).await?;

    let current_files = repository.select_current_files().await;
    let current_stream = current_files.map(|e| e.map(|(_p, b)| RemoveBlob(b)));
    scratch.upsert(current_stream.boxed()).await?;

    let history_stream = repository.select(None).await;
    let history_stream = tokio_stream::StreamExt::filter_map(history_stream, |e| match e {
        Ok(models::File {
            blob_id: Some(blob_id),
            path,
            ..
        }) => Some(Ok(RecordBlobPath(blob_id, path))),
        Ok(models::File { blob_id: None, .. }) => None,
        Err(e) => Some(Err(e)),
    });
    scratch.upsert(history_stream.boxed()).await?;

    let s = scratch.stream().map_err(Into::into);
    let close = move || {
        async {
            let scratch = scratch;
            scratch.close().map_err(Into::into).await
        }
        .boxed()
    };
    Ok((Box::new(close), s.boxed()))
}
