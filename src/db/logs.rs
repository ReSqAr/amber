use crate::db::error::DBError;
use crate::db::models::{Blob, File, InsertMaterialisation, RepositoryName};
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
        let blobs_path = path.join("log/blobs");
        let blobs_cfg = StreamConfig::builder(&blobs_path)
            .compression(Compression::Zstd { level: 3 })
            .build();
        let blobs_writer =
            AsyncStreamWriter::<SerdeBincode<Blob>>::open(blobs_cfg, SerdeBincode::new()).await?;

        let files_path = path.join("log/files");
        let files_cfg = StreamConfig::builder(&files_path)
            .compression(Compression::Zstd { level: 3 })
            .build();
        let files_writer =
            AsyncStreamWriter::<SerdeBincode<File>>::open(files_cfg, SerdeBincode::new()).await?;

        let materialisations_path = path.join("log/materialisations");
        let materialisations_cfg = StreamConfig::builder(&materialisations_path)
            .compression(Compression::Zstd { level: 3 })
            .build();
        let materialisations_writer =
            AsyncStreamWriter::<SerdeBincode<InsertMaterialisation>>::open(
                materialisations_cfg,
                SerdeBincode::new(),
            )
            .await?;

        let repository_names_path = path.join("log/repository_names");
        let repository_names_cfg = StreamConfig::builder(&repository_names_path)
            .compression(Compression::Zstd { level: 3 })
            .build();
        let repository_names_writer = AsyncStreamWriter::<SerdeBincode<RepositoryName>>::open(
            repository_names_cfg,
            SerdeBincode::new(),
        )
        .await?;

        Ok(Self {
            blobs_writer,
            files_writer,
            materialisations_writer,
            repository_names_writer,
            flush_size: 10000,
        })
    }
}
