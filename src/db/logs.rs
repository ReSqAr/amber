use crate::db::error::DBError;
use crate::db::models::{Blob, File, InsertMaterialisation, RepositoryName};
use crate::flightdeck::tracer::Tracer;
use behemoth::{AsyncStreamWriter, Compression, SerdeBincode, StreamConfig};
use std::path::Path;

#[derive(Clone)]
pub(crate) struct Logs {
    pub(crate) blobs_writer: AsyncStreamWriter<SerdeBincode<Blob>>,
    pub(crate) files_writer: AsyncStreamWriter<SerdeBincode<File>>,
    pub(crate) materialisations_writer: AsyncStreamWriter<SerdeBincode<InsertMaterialisation>>,
    pub(crate) repository_names_writer: AsyncStreamWriter<SerdeBincode<RepositoryName>>,
    pub(crate) flush_size: usize,
}

impl Logs {
    pub(crate) async fn new(path: &Path) -> Result<Self, DBError> {
        let tracer = Tracer::new_on("Logs::open");

        let blobs_cfg = StreamConfig::builder(path.join("log/blobs"))
            .compression(Compression::Zstd { level: 3 })
            .build();
        let blobs_writer =
            AsyncStreamWriter::<SerdeBincode<Blob>>::open(blobs_cfg, SerdeBincode::new());

        let files_cfg = StreamConfig::builder(path.join("log/files"))
            .compression(Compression::Zstd { level: 3 })
            .build();
        let files_writer =
            AsyncStreamWriter::<SerdeBincode<File>>::open(files_cfg, SerdeBincode::new());

        let materialisations_cfg = StreamConfig::builder(path.join("log/materialisations"))
            .compression(Compression::Zstd { level: 3 })
            .build();
        let materialisations_writer =
            AsyncStreamWriter::<SerdeBincode<InsertMaterialisation>>::open(
                materialisations_cfg,
                SerdeBincode::new(),
            );

        let repository_names_cfg = StreamConfig::builder(path.join("log/repository_names"))
            .compression(Compression::Zstd { level: 3 })
            .build();
        let repository_names_writer = AsyncStreamWriter::<SerdeBincode<RepositoryName>>::open(
            repository_names_cfg,
            SerdeBincode::new(),
        );

        let (blobs_writer, files_writer, materialisations_writer, repository_names_writer) = tokio::try_join!(
            blobs_writer,
            files_writer,
            materialisations_writer,
            repository_names_writer,
        )?;
        tracer.measure();

        Ok(Self {
            blobs_writer,
            files_writer,
            materialisations_writer,
            repository_names_writer,
            flush_size: 10000,
        })
    }
}
