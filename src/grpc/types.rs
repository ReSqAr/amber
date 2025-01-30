use crate::db::models::{
    Blob as DbBlob, File as DbFile, Repository as DbRepository, TransferItem as DbTransferItem,
};
use crate::grpc::definitions::{Blob, File, Repository, TransferItem};
use chrono::{DateTime, TimeZone, Utc};
use prost_types::Timestamp;

fn datetime_to_timestamp(dt: &DateTime<Utc>) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    })
}

fn timestamp_to_datetime(ts: &Option<Timestamp>) -> DateTime<Utc> {
    ts.and_then(|ts| Utc.timestamp_opt(ts.seconds, ts.nanos as u32).single())
        .unwrap_or_default()
}

impl From<DbRepository> for Repository {
    fn from(repo: DbRepository) -> Self {
        Repository {
            repo_id: repo.repo_id,
            last_file_index: repo.last_file_index,
            last_blob_index: repo.last_blob_index,
        }
    }
}

impl From<DbFile> for File {
    fn from(file: DbFile) -> Self {
        File {
            uuid: file.uuid,
            path: file.path,
            blob_id: file.blob_id,
            valid_from: datetime_to_timestamp(&file.valid_from),
        }
    }
}

impl From<DbBlob> for Blob {
    fn from(blob: DbBlob) -> Self {
        Blob {
            uuid: blob.uuid,
            repo_id: blob.repo_id,
            blob_id: blob.blob_id,
            blob_size: blob.blob_size,
            has_blob: blob.has_blob,
            valid_from: datetime_to_timestamp(&blob.valid_from),
        }
    }
}

impl From<Repository> for DbRepository {
    fn from(repo: Repository) -> Self {
        DbRepository {
            repo_id: repo.repo_id,
            last_file_index: repo.last_file_index,
            last_blob_index: repo.last_blob_index,
        }
    }
}

impl From<File> for DbFile {
    fn from(file: File) -> Self {
        DbFile {
            uuid: file.uuid,
            path: file.path,
            blob_id: file.blob_id,
            valid_from: timestamp_to_datetime(&file.valid_from),
        }
    }
}

impl From<Blob> for DbBlob {
    fn from(blob: Blob) -> Self {
        DbBlob {
            uuid: blob.uuid,
            repo_id: blob.repo_id,
            blob_id: blob.blob_id,
            blob_size: blob.blob_size,
            has_blob: blob.has_blob,
            valid_from: timestamp_to_datetime(&blob.valid_from),
        }
    }
}

impl From<TransferItem> for DbTransferItem {
    fn from(i: TransferItem) -> Self {
        Self {
            transfer_id: i.transfer_id,
            blob_id: i.blob_id,
            path: i.path,
        }
    }
}

impl From<DbTransferItem> for TransferItem {
    fn from(i: DbTransferItem) -> Self {
        Self {
            transfer_id: i.transfer_id,
            blob_id: i.blob_id,
            path: i.path,
        }
    }
}
