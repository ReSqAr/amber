use crate::db::error::DBError;
use crate::db::logstore;
use crate::db::models::{Blob, File, InsertMaterialisation, RepositoryName};
use crate::flightdeck::tracer::Tracer;
use std::path::PathBuf;
use tokio::task;

#[derive(Clone)]
pub(crate) struct Logs {
    pub(crate) blobs_writer: logstore::Writer<Blob>,
    pub(crate) files_writer: logstore::Writer<File>,
    pub(crate) materialisations_writer: logstore::Writer<InsertMaterialisation>,
    pub(crate) repository_names_writer: logstore::Writer<RepositoryName>,
    pub(crate) flush_size: usize,
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

        let rn_path = base_path.join("repository_names.log");
        let rn_writer =
            task::spawn_blocking(move || logstore::Writer::<RepositoryName>::open(rn_path));

        let (b_writer, f_writer, m_writer, rn_writer) =
            tokio::try_join!(b_writer, f_writer, m_writer, rn_writer)?;
        tracer.measure();

        Ok(Self {
            blobs_writer: b_writer?,
            files_writer: f_writer?,
            materialisations_writer: m_writer?,
            repository_names_writer: rn_writer?,
            flush_size: 10000,
        })
    }
}
