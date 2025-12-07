use crate::db::error::DBError;
use crate::db::models::{BlobMeta, BlobRef, FileBlobID, HasBlob, LogRepositoryName, Path, RepoID};
use crate::db::stores::reduced;
use crate::db::stores::reduced::{Always, TransactionLike};
use crate::flightdeck::tracer::Tracer;
use futures::StreamExt;
use futures::try_join;
use futures_core::stream::BoxStream;
use std::path::PathBuf;

#[derive(Clone)]
pub(crate) struct Reduced {
    pub(crate) blobs: reduced::Store<BlobRef, BlobMeta, HasBlob>,
    pub(crate) files: reduced::Store<Path, (), FileBlobID>,
    pub(crate) repository_names: reduced::Store<RepoID, LogRepositoryName, Always>,
    pub(crate) flush_size: usize,
}

impl Reduced {
    pub(crate) async fn new(base_path: PathBuf) -> Result<Self, DBError> {
        let tracer = Tracer::new_on("Logs::open");

        let b_path = base_path.join("blobs");
        let b = reduced::Store::open("blobs".to_string(), b_path);
        let f_path = base_path.join("files");
        let f = reduced::Store::open("files".to_string(), f_path);
        let rn_path = base_path.join("repository_names");
        let rn = reduced::Store::open("repository_names".to_string(), rn_path);

        let (b, f, rn) = tokio::try_join!(b, f, rn)?;
        tracer.measure();

        Ok(Self {
            blobs: b,
            files: f,
            repository_names: rn,
            flush_size: 10000,
        })
    }

    pub(crate) async fn close(&self) -> Result<(), DBError> {
        try_join!(
            self.blobs.close(),
            self.files.close(),
            self.repository_names.close(),
        )?;
        Ok(())
    }

    pub(crate) async fn compact(&self) -> Result<(), DBError> {
        try_join!(
            self.blobs.compact(),
            self.files.compact(),
            self.repository_names.compact(),
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
    let tracer = Tracer::new_on(format!("Database::{}", name));
    let mut txn = txn;

    let mut total_inserted: u64 = 0;
    let mut tracer_put = Tracer::new_off(format!("Database::{}::put", name));
    let mut tracer_flush = Tracer::new_off(format!("Database::{}::flush", name));
    let mut s = s.enumerate();
    while let Some((i, row)) = s.next().await {
        tracer_put.on();
        txn.put(&row?)
            .await
            .inspect_err(|e| log::error!("Database::{} failed to add log: {e}", name))?;
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

    let tracer_commit = Tracer::new_on(format!("Database::{}::commit", name));
    txn.close().await?;
    tracer_commit.measure();

    tracer.measure();
    Ok(total_inserted)
}
