use crate::db::models::{Blob, File, RepositoryMetadata, RepositorySyncState};
use crate::repository::traits::{Syncer, SyncerParams};
use crate::utils::errors::InternalError;
use crate::utils::path::RepoPath;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use futures::{FutureExt, StreamExt, future::BoxFuture};
use futures_core::stream::BoxStream;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::{ArrowWriter, ArrowWriterOptions};
use parquet::file::properties::WriterProperties;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};
use tokio::sync::mpsc;
use tokio::task;

const DEFAULT_BUFFER_SIZE: usize = 100;
const FLUSH_ROWS: usize = 16384;

pub trait ParquetRecord: Sized + Send + Sync + 'static {
    fn schema() -> SchemaRef;
    fn to_record_batch_many(items: &[Self]) -> Result<RecordBatch, InternalError>;
    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self>, InternalError>;
}

#[derive(Debug, Clone)]
pub struct Parquet<T> {
    path: RepoPath,
    _marker: PhantomData<T>,
}

impl<T> Parquet<T> {
    pub fn new(path: RepoPath) -> Self {
        Self {
            path,
            _marker: PhantomData,
        }
    }
}

impl<T> Syncer<T> for Parquet<T>
where
    T: ParquetRecord + SyncerParams,
{
    fn select(
        &self,
        _params: <T as SyncerParams>::Params,
    ) -> BoxFuture<'_, BoxStream<'static, Result<T, InternalError>>> {
        let path = self.path.clone();
        let type_name = std::any::type_name::<T>();

        async move {
            let (tx, rx) = crate::flightdeck::tracked::mpsc_channel(
                format!("Parquet<{type_name}>::select"),
                DEFAULT_BUFFER_SIZE,
            );

            task::spawn_blocking(move || {
                let work: Result<(), InternalError> = (|| {
                    let path = path.abs();
                    if !path.try_exists()? {
                        return Ok(());
                    }

                    let file = std::fs::File::open(path)?;
                    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                        .map_err(|e| parquet_error("open parquet reader", e))?
                        .with_batch_size(DEFAULT_BUFFER_SIZE);

                    let mut reader = builder
                        .build()
                        .map_err(|e| parquet_error("build parquet reader", e))?;

                    for batch in reader.by_ref() {
                        let batch = batch.map_err(|e| parquet_error("read batch", e))?;

                        for item in T::from_batch(&batch)? {
                            tx.blocking_send(Ok(item))?;
                        }
                    }

                    Ok(())
                })();

                if let Err(e) = work {
                    log::error!("failed to read parquet file {}: {}", path.display(), e);
                    let _ = tx.blocking_send(Err(e));
                }
            });

            rx.boxed()
        }
        .boxed()
    }

    fn merge(&self, s: BoxStream<'static, T>) -> BoxFuture<'_, Result<(), InternalError>> {
        let path = self.path.clone();

        async move {
            let (tx, mut rx) = mpsc::channel::<Result<T, InternalError>>(DEFAULT_BUFFER_SIZE);

            let bg = task::spawn_blocking(move || -> Result<(), InternalError> {
                let file = std::fs::File::create_new(path.abs())?;

                let props = WriterProperties::builder().build();
                let options = ArrowWriterOptions::new().with_properties(props);

                let mut writer = ArrowWriter::try_new_with_options(file, T::schema(), options)
                    .map_err(|e| parquet_error("create writer", e))?;

                let mut buf: Vec<T> = Vec::with_capacity(FLUSH_ROWS);

                while let Some(msg) = rx.blocking_recv() {
                    match msg {
                        Ok(item) => {
                            buf.push(item);
                            if buf.len() >= FLUSH_ROWS {
                                let batch = T::to_record_batch_many(&buf)?;
                                writer
                                    .write(&batch)
                                    .map_err(|e| parquet_error("write batch", e))?;
                                buf.clear();
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }

                if !buf.is_empty() {
                    let batch = T::to_record_batch_many(&buf)?;
                    writer
                        .write(&batch)
                        .map_err(|e| parquet_error("write batch", e))?;
                }

                writer
                    .close()
                    .map_err(|e| parquet_error("close parquet writer", e))?;

                Ok(())
            });

            let mut stream = s;
            while let Some(item) = stream.next().await {
                tx.send(Ok(item)).await?;
            }
            drop(tx);

            bg.await??;
            Ok(())
        }
        .boxed()
    }
}

macro_rules! parquet_record {
    ($ty:ty, $fields:expr) => {
        impl ParquetRecord for $ty {
            fn schema() -> SchemaRef {
                static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
                SCHEMA
                    .get_or_init(|| Arc::new(Schema::new($fields)))
                    .clone()
            }

            fn to_record_batch_many(items: &[Self]) -> Result<RecordBatch, InternalError> {
                let schema = Self::schema();
                serde_arrow::to_record_batch(schema.fields(), &items)
                    .map_err(|e| parquet_error("build record batch", e))
            }

            fn from_batch(batch: &RecordBatch) -> Result<Vec<Self>, InternalError> {
                serde_arrow::from_record_batch(batch)
                    .map_err(|e| parquet_error("read record batch", e))
            }
        }
    };
}

parquet_record!(
    File,
    vec![
        Field::new("uid", DataType::UInt64, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("blob_id", DataType::Utf8, true),
        Field::new("valid_from", utc_timestamp(), false),
    ]
);

parquet_record!(
    Blob,
    vec![
        Field::new("uid", DataType::UInt64, false),
        Field::new("repo_id", DataType::Utf8, false),
        Field::new("blob_id", DataType::Utf8, false),
        Field::new("blob_size", DataType::UInt64, false),
        Field::new("has_blob", DataType::Boolean, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("valid_from", utc_timestamp(), false),
    ]
);

parquet_record!(
    RepositoryMetadata,
    vec![
        Field::new("uid", DataType::UInt64, false),
        Field::new("repo_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("valid_from", utc_timestamp(), false),
    ]
);

parquet_record!(
    RepositorySyncState,
    vec![
        Field::new("repo_id", DataType::Utf8, false),
        Field::new("last_file_index", DataType::UInt64, true),
        Field::new("last_blob_index", DataType::UInt64, true),
        Field::new("last_name_index", DataType::UInt64, true),
    ]
);

fn parquet_error(context: &str, e: impl std::fmt::Display) -> InternalError {
    InternalError::Parquet {
        context: context.to_string(),
        error: e.to_string(),
    }
}

/// The timestamp type the stores have always used; serde_arrow converts the
/// RFC 3339 strings chrono serialises into it, and back.
fn utc_timestamp() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
}

#[allow(clippy::indexing_slicing)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::models::{BlobID, Path as ModelPath, RepoID, Uid};
    use futures::TryStreamExt;
    use futures::stream;
    use tempfile::tempdir;

    /// Files written before the codecs moved to serde_arrow have to stay
    /// readable: they are sitting on people's rclone remotes. The fixture is
    /// produced by the hand-written writer this replaces.
    #[tokio::test]
    async fn reads_a_legacy_file() -> Result<(), InternalError> {
        let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let path = RepoPath::from_root(fixtures).join("legacy_blobs.parquet");
        let parquet = Parquet::<Blob>::new(path);
        let items: Vec<Blob> = parquet.select(None).await.try_collect().await?;

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].uid, Uid(0));
        assert_eq!(items[0].repo_id, RepoID("repo-abc".to_string()));
        assert_eq!(items[0].blob_id, BlobID("blob0".to_string()));
        assert_eq!(items[0].blob_size, 100);
        assert!(items[0].has_blob);
        assert_eq!(
            items[0].path.as_ref().map(|p| p.0.clone()),
            Some("p/0.bin".to_string())
        );
        assert_eq!(
            items[0].valid_from.timestamp_nanos_opt(),
            Some(1_700_000_000_000_000_123)
        );

        assert!(items[1].path.is_none());
        assert!(!items[1].has_blob);
        assert_eq!(items[2].blob_id, BlobID("blob2".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn parquet_file_round_trip() -> Result<(), InternalError> {
        let temp = tempdir().map_err(InternalError::IO)?;
        let path = RepoPath::from_root(temp.path()).join("files.parquet");
        let parquet = Parquet::<File>::new(path);

        let item = File {
            uid: Uid(10),
            path: ModelPath("hello.txt".to_string()),
            blob_id: Some(BlobID("blob123".to_string())),
            valid_from: chrono::Utc::now(),
        };

        parquet
            .merge(stream::iter(vec![item.clone()]).boxed())
            .await?;
        let stream = parquet.select(None).await;
        let items: Vec<_> = stream.try_collect().await?;

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].uid, item.uid);
        assert_eq!(items[0].path.0, item.path.0);
        assert_eq!(
            items[0].blob_id.as_ref().unwrap().0,
            item.blob_id.unwrap().0
        );
        assert_eq!(
            items[0].valid_from.timestamp_nanos_opt().unwrap(),
            item.valid_from.timestamp_nanos_opt().unwrap()
        );
        Ok(())
    }

    #[tokio::test]
    async fn parquet_blob_round_trip() -> Result<(), InternalError> {
        let temp = tempdir().map_err(InternalError::IO)?;
        let path = RepoPath::from_root(temp.path()).join("blobs.parquet");
        let parquet = Parquet::<Blob>::new(path);

        let item = Blob {
            uid: Uid(5),
            repo_id: RepoID("repo".to_string()),
            blob_id: BlobID("blob".to_string()),
            blob_size: 42,
            has_blob: true,
            path: Some(ModelPath("dir/file.bin".to_string())),
            valid_from: chrono::Utc::now(),
        };

        parquet
            .merge(stream::iter(vec![item.clone()]).boxed())
            .await?;
        let stream = parquet.select(None).await;
        let items: Vec<_> = stream.try_collect().await?;

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].uid, item.uid);
        assert_eq!(items[0].repo_id, item.repo_id);
        assert_eq!(items[0].blob_id, item.blob_id);
        assert_eq!(items[0].blob_size, item.blob_size);
        assert_eq!(items[0].has_blob, item.has_blob);
        assert_eq!(items[0].path.as_ref().unwrap().0, item.path.unwrap().0);
        Ok(())
    }

    #[tokio::test]
    async fn parquet_repository_metadata_round_trip() -> Result<(), InternalError> {
        let temp = tempdir().map_err(InternalError::IO)?;
        let path = RepoPath::from_root(temp.path()).join("repository_metadata.parquet");
        let parquet = Parquet::<RepositoryMetadata>::new(path);

        let item0 = RepositoryMetadata {
            uid: Uid(99),
            repo_id: RepoID("repo".to_string()),
            name: Some("example".to_string()),
            valid_from: chrono::Utc::now(),
        };

        let item1 = RepositoryMetadata {
            uid: Uid(100),
            repo_id: RepoID("repo".to_string()),
            name: None,
            valid_from: chrono::Utc::now(),
        };

        parquet
            .merge(stream::iter(vec![item0.clone(), item1.clone()]).boxed())
            .await?;
        let stream = parquet.select(None).await;
        let items: Vec<_> = stream.try_collect().await?;

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].uid, item0.uid);
        assert_eq!(items[0].repo_id, item0.repo_id);
        assert_eq!(items[0].name, item0.name);
        assert_eq!(items[1].uid, item1.uid);
        assert_eq!(items[1].repo_id, item1.repo_id);
        assert_eq!(items[1].name, item1.name);
        Ok(())
    }

    #[tokio::test]
    async fn parquet_sync_state_round_trip() -> Result<(), InternalError> {
        let temp = tempdir().map_err(InternalError::IO)?;
        let path = RepoPath::from_root(temp.path()).join("sync_state.parquet");
        let parquet = Parquet::<RepositorySyncState>::new(path);

        let item = RepositorySyncState {
            repo_id: RepoID("repo".to_string()),
            last_file_index: Some(1),
            last_blob_index: None,
            last_name_index: Some(3),
        };

        parquet
            .merge(stream::iter(vec![item.clone()]).boxed())
            .await?;
        let stream = parquet.select(()).await;
        let items: Vec<_> = stream.try_collect().await?;

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].repo_id, item.repo_id);
        assert_eq!(items[0].last_file_index, item.last_file_index);
        assert_eq!(items[0].last_blob_index, item.last_blob_index);
        assert_eq!(items[0].last_name_index, item.last_name_index);
        Ok(())
    }
}
