use crate::db::models;
use crate::flightdeck;
use crate::grpc::definitions::flightdeck_data::Value;
use crate::grpc::definitions::{
    Blob, CopiedTransferItem, File, FlightdeckData, FlightdeckMessage, FlightdeckObservation,
    Repository, RepositoryName, TransferItem,
};
use chrono::{DateTime, TimeZone, Utc};
use prost_types::Timestamp;
use std::str::FromStr;

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

impl From<FlightdeckMessage> for flightdeck::observation::Message {
    fn from(m: FlightdeckMessage) -> Self {
        Self {
            level: log::Level::from_str(&m.level).unwrap_or(log::Level::Info),
            observation: m.observation.unwrap().into(),
        }
    }
}

impl From<flightdeck::observation::Message> for FlightdeckMessage {
    fn from(m: flightdeck::observation::Message) -> Self {
        Self {
            level: m.level.to_string(),
            observation: Some(m.observation.into()),
        }
    }
}

impl From<FlightdeckObservation> for flightdeck::observation::Observation {
    fn from(o: FlightdeckObservation) -> Self {
        Self {
            type_key: o.type_key,
            id: o.id,
            timestamp: timestamp_to_datetime(&o.timestamp),
            is_terminal: o.is_terminal,
            data: o.data.into_iter().map(|d| d.into()).collect(),
        }
    }
}

impl From<flightdeck::observation::Observation> for FlightdeckObservation {
    fn from(o: flightdeck::observation::Observation) -> Self {
        Self {
            type_key: o.type_key,
            id: o.id,
            timestamp: datetime_to_timestamp(&o.timestamp),
            is_terminal: o.is_terminal,
            data: o.data.into_iter().map(|d| d.into()).collect(),
        }
    }
}

impl From<FlightdeckData> for flightdeck::observation::Data {
    fn from(m: FlightdeckData) -> Self {
        Self {
            key: m.key,
            value: m.value.unwrap().into(),
        }
    }
}

impl From<flightdeck::observation::Data> for FlightdeckData {
    fn from(m: flightdeck::observation::Data) -> Self {
        Self {
            key: m.key,
            value: Some(m.value.into()),
        }
    }
}

impl From<Value> for flightdeck::observation::Value {
    fn from(m: Value) -> Self {
        match m {
            Value::String(s) => flightdeck::observation::Value::String(s),
            Value::U64(u64) => flightdeck::observation::Value::U64(u64),
            Value::Bool(b) => flightdeck::observation::Value::Bool(b),
        }
    }
}

impl From<flightdeck::observation::Value> for Value {
    fn from(m: flightdeck::observation::Value) -> Self {
        match m {
            flightdeck::observation::Value::String(s) => Value::String(s),
            flightdeck::observation::Value::U64(u64) => Value::U64(u64),
            flightdeck::observation::Value::Bool(b) => Value::Bool(b),
        }
    }
}
