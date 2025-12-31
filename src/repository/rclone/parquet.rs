use crate::db::models::{
    Blob, BlobID, File, Path as ModelPath, RepoID, RepositoryName, RepositorySyncState, Uid,
};
use crate::repository::traits::{Syncer, SyncerParams};
use crate::utils::errors::InternalError;
use crate::utils::path::RepoPath;
use arrow_array::builder::{
    BooleanBuilder, StringBuilder, TimestampNanosecondBuilder, UInt64Builder,
};
use arrow_array::{
    Array, BooleanArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array,
};
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

impl ParquetRecord for File {
    fn schema() -> SchemaRef {
        static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
        SCHEMA
            .get_or_init(|| {
                Arc::new(Schema::new(vec![
                    Field::new("uid", DataType::UInt64, false),
                    Field::new("path", DataType::Utf8, false),
                    Field::new("blob_id", DataType::Utf8, true),
                    Field::new(
                        "valid_from",
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                        false,
                    ),
                ]))
            })
            .clone()
    }

    fn to_record_batch_many(items: &[Self]) -> Result<RecordBatch, InternalError> {
        let n = items.len();

        let mut uid_b = UInt64Builder::with_capacity(n);
        let mut path_b = StringBuilder::with_capacity(n, n * 32);
        let mut blob_id_b = StringBuilder::with_capacity(n, n * 32);
        let mut ts_b = utc_ts_builder(n);

        for it in items {
            uid_b.append_value(it.uid.0);
            path_b.append_value(&it.path.0);

            if let Some(id) = &it.blob_id {
                blob_id_b.append_value(&id.0);
            } else {
                blob_id_b.append_null();
            }

            ts_b.append_value(it.valid_from.timestamp_nanos_opt().unwrap());
        }

        let valid_from = finish_ts_utc(ts_b);

        RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(uid_b.finish()),
                Arc::new(path_b.finish()),
                Arc::new(blob_id_b.finish()),
                Arc::new(valid_from),
            ],
        )
        .map_err(|e| parquet_error("build record batch", e))
    }

    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self>, InternalError> {
        let uid = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| InternalError::Stream("uid column missing".into()))?;

        let path = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| InternalError::Stream("path column missing".into()))?;

        let blob_id = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| InternalError::Stream("blob_id column missing".into()))?;

        let valid_from = batch
            .column(3)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| InternalError::Stream("valid_from column missing".into()))?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if uid.is_null(row) {
                return Err(InternalError::Stream("unexpected null: uid".into()));
            }
            if path.is_null(row) {
                return Err(InternalError::Stream("unexpected null: path".into()));
            }
            if valid_from.is_null(row) {
                return Err(InternalError::Stream("unexpected null: valid_from".into()));
            }

            let nanos = valid_from.value(row);
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(nanos);

            out.push(File {
                uid: Uid(uid.value(row)),
                path: ModelPath(path.value(row).to_string()),
                blob_id: if blob_id.is_null(row) {
                    None
                } else {
                    Some(BlobID(blob_id.value(row).to_string()))
                },
                valid_from: dt,
            });
        }
        Ok(out)
    }
}

impl ParquetRecord for Blob {
    fn schema() -> SchemaRef {
        static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
        SCHEMA
            .get_or_init(|| {
                Arc::new(Schema::new(vec![
                    Field::new("uid", DataType::UInt64, false),
                    Field::new("repo_id", DataType::Utf8, false),
                    Field::new("blob_id", DataType::Utf8, false),
                    Field::new("blob_size", DataType::UInt64, false),
                    Field::new("has_blob", DataType::Boolean, false),
                    Field::new("path", DataType::Utf8, true),
                    Field::new(
                        "valid_from",
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                        false,
                    ),
                ]))
            })
            .clone()
    }

    fn to_record_batch_many(items: &[Self]) -> Result<RecordBatch, InternalError> {
        let n = items.len();

        let mut uid_b = UInt64Builder::with_capacity(n);
        let mut repo_id_b = StringBuilder::with_capacity(n, n * 32);
        let mut blob_id_b = StringBuilder::with_capacity(n, n * 32);
        let mut blob_size_b = UInt64Builder::with_capacity(n);
        let mut has_blob_b = BooleanBuilder::with_capacity(n);
        let mut path_b = StringBuilder::with_capacity(n, n * 32);
        let mut ts_b = utc_ts_builder(n);

        for it in items {
            uid_b.append_value(it.uid.0);
            repo_id_b.append_value(&it.repo_id.0);
            blob_id_b.append_value(&it.blob_id.0);
            blob_size_b.append_value(it.blob_size);
            has_blob_b.append_value(it.has_blob);

            if let Some(p) = &it.path {
                path_b.append_value(&p.0);
            } else {
                path_b.append_null();
            }

            ts_b.append_value(it.valid_from.timestamp_nanos_opt().unwrap());
        }

        let valid_from = finish_ts_utc(ts_b);

        RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(uid_b.finish()),
                Arc::new(repo_id_b.finish()),
                Arc::new(blob_id_b.finish()),
                Arc::new(blob_size_b.finish()),
                Arc::new(has_blob_b.finish()),
                Arc::new(path_b.finish()),
                Arc::new(valid_from),
            ],
        )
        .map_err(|e| parquet_error("build record batch", e))
    }

    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self>, InternalError> {
        let uid = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| InternalError::Stream("uid column missing".into()))?;
        let repo_id = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| InternalError::Stream("repo_id column missing".into()))?;
        let blob_id = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| InternalError::Stream("blob_id column missing".into()))?;
        let blob_size = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| InternalError::Stream("blob_size column missing".into()))?;
        let has_blob = batch
            .column(4)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| InternalError::Stream("has_blob column missing".into()))?;
        let path = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| InternalError::Stream("path column missing".into()))?;
        let valid_from = batch
            .column(6)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| InternalError::Stream("valid_from column missing".into()))?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if uid.is_null(row) {
                return Err(InternalError::Stream("unexpected null: uid".into()));
            }
            if repo_id.is_null(row) {
                return Err(InternalError::Stream("unexpected null: repo_id".into()));
            }
            if blob_id.is_null(row) {
                return Err(InternalError::Stream("unexpected null: blob_id".into()));
            }
            if blob_size.is_null(row) {
                return Err(InternalError::Stream("unexpected null: blob_size".into()));
            }
            if has_blob.is_null(row) {
                return Err(InternalError::Stream("unexpected null: has_blob".into()));
            }
            if valid_from.is_null(row) {
                return Err(InternalError::Stream("unexpected null: valid_from".into()));
            }

            let nanos = valid_from.value(row);
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(nanos);

            out.push(Blob {
                uid: Uid(uid.value(row)),
                repo_id: RepoID(repo_id.value(row).to_string()),
                blob_id: BlobID(blob_id.value(row).to_string()),
                blob_size: blob_size.value(row),
                has_blob: has_blob.value(row),
                path: if path.is_null(row) {
                    None
                } else {
                    Some(ModelPath(path.value(row).to_string()))
                },
                valid_from: dt,
            });
        }
        Ok(out)
    }
}

impl ParquetRecord for RepositoryName {
    fn schema() -> SchemaRef {
        static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
        SCHEMA
            .get_or_init(|| {
                Arc::new(Schema::new(vec![
                    Field::new("uid", DataType::UInt64, false),
                    Field::new("repo_id", DataType::Utf8, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new(
                        "valid_from",
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                        false,
                    ),
                ]))
            })
            .clone()
    }

    fn to_record_batch_many(items: &[Self]) -> Result<RecordBatch, InternalError> {
        let n = items.len();

        let mut uid_b = UInt64Builder::with_capacity(n);
        let mut repo_id_b = StringBuilder::with_capacity(n, n * 32);
        let mut name_b = StringBuilder::with_capacity(n, n * 32);
        let mut ts_b = utc_ts_builder(n);

        for it in items {
            uid_b.append_value(it.uid.0);
            repo_id_b.append_value(&it.repo_id.0);
            name_b.append_value(&it.name);
            ts_b.append_value(it.valid_from.timestamp_nanos_opt().unwrap());
        }

        let valid_from = finish_ts_utc(ts_b);

        RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(uid_b.finish()),
                Arc::new(repo_id_b.finish()),
                Arc::new(name_b.finish()),
                Arc::new(valid_from),
            ],
        )
        .map_err(|e| parquet_error("build record batch", e))
    }

    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self>, InternalError> {
        let uid = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| InternalError::Stream("uid column missing".into()))?;
        let repo_id = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| InternalError::Stream("repo_id column missing".into()))?;
        let name = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| InternalError::Stream("name column missing".into()))?;
        let valid_from = batch
            .column(3)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| InternalError::Stream("valid_from column missing".into()))?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if uid.is_null(row) {
                return Err(InternalError::Stream("unexpected null: uid".into()));
            }
            if repo_id.is_null(row) {
                return Err(InternalError::Stream("unexpected null: repo_id".into()));
            }
            if name.is_null(row) {
                return Err(InternalError::Stream("unexpected null: name".into()));
            }
            if valid_from.is_null(row) {
                return Err(InternalError::Stream("unexpected null: valid_from".into()));
            }

            let nanos = valid_from.value(row);
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(nanos);

            out.push(RepositoryName {
                uid: Uid(uid.value(row)),
                repo_id: RepoID(repo_id.value(row).to_string()),
                name: name.value(row).to_string(),
                valid_from: dt,
            });
        }
        Ok(out)
    }
}

impl ParquetRecord for RepositorySyncState {
    fn schema() -> SchemaRef {
        static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
        SCHEMA
            .get_or_init(|| {
                Arc::new(Schema::new(vec![
                    Field::new("repo_id", DataType::Utf8, false),
                    Field::new("last_file_index", DataType::UInt64, true),
                    Field::new("last_blob_index", DataType::UInt64, true),
                    Field::new("last_name_index", DataType::UInt64, true),
                ]))
            })
            .clone()
    }

    fn to_record_batch_many(items: &[Self]) -> Result<RecordBatch, InternalError> {
        let n = items.len();

        let mut repo_id_b = StringBuilder::with_capacity(n, n * 32);
        let mut last_file_b = UInt64Builder::with_capacity(n);
        let mut last_blob_b = UInt64Builder::with_capacity(n);
        let mut last_name_b = UInt64Builder::with_capacity(n);

        for it in items {
            repo_id_b.append_value(&it.repo_id.0);

            match it.last_file_index {
                Some(v) => last_file_b.append_value(v),
                None => last_file_b.append_null(),
            }
            match it.last_blob_index {
                Some(v) => last_blob_b.append_value(v),
                None => last_blob_b.append_null(),
            }
            match it.last_name_index {
                Some(v) => last_name_b.append_value(v),
                None => last_name_b.append_null(),
            }
        }

        RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(repo_id_b.finish()),
                Arc::new(last_file_b.finish()),
                Arc::new(last_blob_b.finish()),
                Arc::new(last_name_b.finish()),
            ],
        )
        .map_err(|e| parquet_error("build record batch", e))
    }

    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self>, InternalError> {
        let repo_id = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| InternalError::Stream("repo_id column missing".into()))?;
        let last_file = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| InternalError::Stream("last_file_index column missing".into()))?;
        let last_blob = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| InternalError::Stream("last_blob_index column missing".into()))?;
        let last_name = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| InternalError::Stream("last_name_index column missing".into()))?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if repo_id.is_null(row) {
                return Err(InternalError::Stream("unexpected null: repo_id".into()));
            }

            out.push(RepositorySyncState {
                repo_id: RepoID(repo_id.value(row).to_string()),
                last_file_index: if last_file.is_null(row) {
                    None
                } else {
                    Some(last_file.value(row))
                },
                last_blob_index: if last_blob.is_null(row) {
                    None
                } else {
                    Some(last_blob.value(row))
                },
                last_name_index: if last_name.is_null(row) {
                    None
                } else {
                    Some(last_name.value(row))
                },
            });
        }

        Ok(out)
    }
}

fn parquet_error(context: &str, e: impl std::fmt::Display) -> InternalError {
    InternalError::Parquet {
        context: context.to_string(),
        error: e.to_string(),
    }
}

fn utc_ts_builder(len: usize) -> TimestampNanosecondBuilder {
    TimestampNanosecondBuilder::with_capacity(len)
}

fn finish_ts_utc(mut b: TimestampNanosecondBuilder) -> TimestampNanosecondArray {
    b.finish().with_timezone("UTC".to_string())
}

#[allow(clippy::indexing_slicing)]
#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use futures::stream;
    use tempfile::tempdir;

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
        assert_eq!(items[0].repo_id.0, item.repo_id.0);
        assert_eq!(items[0].blob_id.0, item.blob_id.0);
        assert_eq!(items[0].blob_size, item.blob_size);
        assert_eq!(items[0].has_blob, item.has_blob);
        assert_eq!(items[0].path.as_ref().unwrap().0, item.path.unwrap().0);
        Ok(())
    }

    #[tokio::test]
    async fn parquet_repository_name_round_trip() -> Result<(), InternalError> {
        let temp = tempdir().map_err(InternalError::IO)?;
        let path = RepoPath::from_root(temp.path()).join("repository_names.parquet");
        let parquet = Parquet::<RepositoryName>::new(path);

        let item = RepositoryName {
            uid: Uid(99),
            repo_id: RepoID("repo".to_string()),
            name: "example".to_string(),
            valid_from: chrono::Utc::now(),
        };

        parquet
            .merge(stream::iter(vec![item.clone()]).boxed())
            .await?;
        let stream = parquet.select(None).await;
        let items: Vec<_> = stream.try_collect().await?;

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].uid, item.uid);
        assert_eq!(items[0].repo_id.0, item.repo_id.0);
        assert_eq!(items[0].name, item.name);
        Ok(())
    }

    #[tokio::test]
    async fn parquet_repository_round_trip() -> Result<(), InternalError> {
        let temp = tempdir().map_err(InternalError::IO)?;
        let path = RepoPath::from_root(temp.path()).join("repositories.parquet");
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
        assert_eq!(items[0].repo_id.0, item.repo_id.0);
        assert_eq!(items[0].last_file_index, item.last_file_index);
        assert_eq!(items[0].last_blob_index, item.last_blob_index);
        assert_eq!(items[0].last_name_index, item.last_name_index);
        Ok(())
    }
}
