use crate::db::error::DBError;
use crate::db::models::{
    BlobMeta, BlobRef, FileBlobID, HasBlob, LogRepositoryMetadataStatus, Path, RepoID,
};
use crate::db::stores::reduced;
use crate::db::stores::reduced::TransactionLike;
use crate::db::versioning::V1;
use crate::flightdeck::tracer::Tracer;
use futures::StreamExt;
use futures::try_join;
use futures_core::stream::BoxStream;
use std::path::PathBuf;

#[derive(Clone)]
pub(crate) struct Reduced {
    pub(crate) blobs: reduced::Store<BlobRef, V1<BlobMeta>, HasBlob>,
    pub(crate) files: reduced::Store<Path, V1<()>, FileBlobID>,
    pub(crate) repository_metadata: reduced::Store<RepoID, V1<()>, LogRepositoryMetadataStatus>,
    pub(crate) flush_size: usize,
}

impl Reduced {
    pub(crate) async fn new(base_path: PathBuf) -> Result<Self, DBError> {
        let tracer = Tracer::new_on("Logs::open");

        let b_path = base_path.join("blobs");
        let b = reduced::Store::open("blobs".to_string(), b_path);
        let f_path = base_path.join("files");
        let f = reduced::Store::open("files".to_string(), f_path);
        let rm_path = base_path.join("repository_metadata");
        let rm = reduced::Store::open("repository_metadata".to_string(), rm_path);

        let (b, f, rm) = tokio::try_join!(b, f, rm)?;
        tracer.measure();

        Ok(Self {
            blobs: b,
            files: f,
            repository_metadata: rm,
            flush_size: 10000,
        })
    }

    pub(crate) async fn close(&self) -> Result<(), DBError> {
        try_join!(
            self.blobs.close(),
            self.files.close(),
            self.repository_metadata.close(),
        )?;
        Ok(())
    }

    pub(crate) async fn compact(&self) -> Result<(), DBError> {
        try_join!(
            self.blobs.compact(),
            self.files.compact(),
            self.repository_metadata.compact(),
        )?;
        Ok(())
    }
}

#[allow(clippy::type_complexity)]
pub(crate) async fn add<V>(
    s: BoxStream<'_, Result<V, DBError>>,
    txn: impl TransactionLike<V>,
    name: String,
    flush_size: usize,
) -> Result<u64, DBError> {
    let tracer = Tracer::new_on(format!("reduced::add::{}", name));
    let mut txn = txn;

    let mut total_inserted: u64 = 0;
    let mut tracer_put = Tracer::new_off(format!("reduced::add::{}::put", name));
    let mut tracer_flush = Tracer::new_off(format!("reduced::add::{}::flush", name));
    let mut s = s.enumerate();
    while let Some((i, row)) = s.next().await {
        tracer_put.on();
        txn.put(&row?)
            .await
            .inspect_err(|e| log::error!("reduced::add::{} failed to add log: {e}", name))?;
        tracer_put.off();
        total_inserted += 1;

        if i % flush_size == 0 {
            tracer_flush.on();
            txn.flush().await?;
            tracer_flush.off();
        }
    }
    tracer_put.measure();
    tracer_flush.measure();

    let tracer_commit = Tracer::new_on(format!("reduced::add::{}::commit", name));
    txn.close().await?;
    tracer_commit.measure();

    tracer.measure();
    Ok(total_inserted)
}
