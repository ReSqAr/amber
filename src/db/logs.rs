use crate::db::error::DBError;
use crate::db::models::{Blob, File, InsertMaterialisation, RawRepositoryName, RepoID, Uid};
use crate::db::reduced::{RowStatus, Status, TransactionLike, ValidFrom};
use crate::db::{logstore, reduced};
use crate::flightdeck::tracer::Tracer;
use futures::StreamExt;
use futures::try_join;
use futures_core::stream::BoxStream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::PathBuf;
use tokio::task;

#[derive(Clone)]
pub(crate) struct Logs {
    pub(crate) repository_names: reduced::Reduced<RepoID, RawRepositoryName, Always>,

    pub(crate) blobs_writer: logstore::Writer<Blob>,
    pub(crate) files_writer: logstore::Writer<File>,
    pub(crate) materialisations_writer: logstore::Writer<InsertMaterialisation>,
    pub(crate) flush_size: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Always();

impl Status for Always {
    fn status(&self) -> RowStatus {
        RowStatus::Keep
    }
}

impl Logs {
    pub(crate) async fn new(base_path: PathBuf) -> Result<Self, DBError> {
        let tracer = Tracer::new_on("Logs::open");

        let b_path = base_path.join("blobs.log");
        let b_writer = task::spawn_blocking(move || logstore::Writer::<Blob>::open(b_path));

        let f_path = base_path.join("files.log");
        let f_writer = task::spawn_blocking(move || logstore::Writer::<File>::open(f_path));

        let m_path = base_path.join("materialisations.log");
        let m_writer =
            task::spawn_blocking(move || logstore::Writer::<InsertMaterialisation>::open(m_path));

        let rn2_path = base_path.join("repository_names");
        let rn2_writer = reduced::Reduced::open("repository_names".to_string(), rn2_path);

        let (b_writer, f_writer, m_writer) = tokio::try_join!(b_writer, f_writer, m_writer)?;

        let (rn2_writer,) = tokio::try_join!(rn2_writer)?;
        tracer.measure();

        Ok(Self {
            repository_names: rn2_writer,
            blobs_writer: b_writer?,
            files_writer: f_writer?,
            materialisations_writer: m_writer?,
            flush_size: 10000,
        })
    }

    pub(crate) async fn close(&self) -> Result<(), DBError> {
        try_join!(self.repository_names.close(),)?;
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    pub(crate) async fn add_repository_names(
        &self,
        s: BoxStream<'_, Result<(Uid, RepoID, RawRepositoryName, Always, ValidFrom), DBError>>,
    ) -> Result<u64, DBError> {
        self.add(
            s,
            self.repository_names.transaction().await?,
            self.repository_names.name().clone(),
        )
        .await
    }

    #[allow(clippy::type_complexity)]
    async fn add<K, V, S>(
        &self,
        s: BoxStream<'_, Result<(Uid, K, V, S, ValidFrom), DBError>>,
        txn: impl TransactionLike<K, V, S>,
        name: String,
    ) -> Result<u64, DBError>
    where
        K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        S: Status + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let tracer = Tracer::new_on(format!("Database::{}", name));
        let mut txn = txn;

        let mut total_inserted: u64 = 0;
        let mut tracer_put = Tracer::new_off(format!("Database::{}::put", name));
        let mut tracer_flush = Tracer::new_off(format!("Database::{}::flush", name));
        let mut s = s.enumerate();
        while let Some((i, row)) = s.next().await {
            let (uid, k, v, status, valid_from) = row?;
            tracer_put.on();
            txn.put(&(uid, k, v, status, valid_from))
                .await
                .inspect_err(|e| log::error!("Database::{} failed to add log: {e}", name))?;
            tracer_put.off();
            total_inserted += 1;

            if i % self.flush_size == 0 {
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
}
