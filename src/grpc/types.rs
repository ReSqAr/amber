use crate::db::models;
use crate::grpc::definitions::{
    Blob, CopiedTransferItem, File, Repository, RepositoryName, TransferItem,
};
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

impl From<models::Repository> for Repository {
    fn from(repo: models::Repository) -> Self {
        Repository {
            repo_id: repo.repo_id,
            last_file_index: repo.last_file_index,
            last_blob_index: repo.last_blob_index,
            last_name_index: repo.last_name_index,
        }
    }
}

impl From<models::File> for File {
    fn from(file: models::File) -> Self {
        File {
            uuid: file.uuid,
            path: file.path,
            blob_id: file.blob_id,
            valid_from: datetime_to_timestamp(&file.valid_from),
        }
    }
}

impl From<models::Blob> for Blob {
    fn from(blob: models::Blob) -> Self {
        Blob {
            uuid: blob.uuid,
            repo_id: blob.repo_id,
            blob_id: blob.blob_id,
            blob_size: blob.blob_size,
            has_blob: blob.has_blob,
            path: blob.path,
            valid_from: datetime_to_timestamp(&blob.valid_from),
        }
    }
}

impl From<models::RepositoryName> for RepositoryName {
    fn from(repo_name: models::RepositoryName) -> Self {
        RepositoryName {
            uuid: repo_name.uuid,
            repo_id: repo_name.repo_id,
            name: repo_name.name,
            valid_from: datetime_to_timestamp(&repo_name.valid_from),
        }
    }
}

impl From<Repository> for models::Repository {
    fn from(repo: Repository) -> Self {
        models::Repository {
            repo_id: repo.repo_id,
            last_file_index: repo.last_file_index,
            last_blob_index: repo.last_blob_index,
            last_name_index: repo.last_name_index,
        }
    }
}

impl From<File> for models::File {
    fn from(file: File) -> Self {
        models::File {
            uuid: file.uuid,
            path: file.path,
            blob_id: file.blob_id,
            valid_from: timestamp_to_datetime(&file.valid_from),
        }
    }
}

impl From<Blob> for models::Blob {
    fn from(blob: Blob) -> Self {
        models::Blob {
            uuid: blob.uuid,
            repo_id: blob.repo_id,
            blob_id: blob.blob_id,
            blob_size: blob.blob_size,
            has_blob: blob.has_blob,
            path: blob.path,
            valid_from: timestamp_to_datetime(&blob.valid_from),
        }
    }
}

impl From<RepositoryName> for models::RepositoryName {
    fn from(repo_name: RepositoryName) -> Self {
        models::RepositoryName {
            uuid: repo_name.uuid,
            repo_id: repo_name.repo_id,
            name: repo_name.name,
            valid_from: timestamp_to_datetime(&repo_name.valid_from),
        }
    }
}

impl From<TransferItem> for models::BlobTransferItem {
    fn from(i: TransferItem) -> Self {
        Self {
            transfer_id: i.transfer_id,
            blob_id: i.blob_id,
            path: i.path,
        }
    }
}

impl From<models::BlobTransferItem> for TransferItem {
    fn from(i: models::BlobTransferItem) -> Self {
        Self {
            transfer_id: i.transfer_id,
            blob_id: i.blob_id,
            path: i.path,
        }
    }
}

impl From<CopiedTransferItem> for models::CopiedTransferItem {
    fn from(i: CopiedTransferItem) -> Self {
        Self {
            transfer_id: i.transfer_id,
            path: i.path,
        }
    }
}

impl From<models::CopiedTransferItem> for CopiedTransferItem {
    fn from(i: models::CopiedTransferItem) -> Self {
        Self {
            transfer_id: i.transfer_id,
            path: i.path,
        }
    }
}
