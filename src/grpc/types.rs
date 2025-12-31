use crate::db::models;
use crate::db::models::{BlobID, RepoID};
use crate::flightdeck;
use crate::grpc::definitions::flightdeck_data::Value;
use crate::grpc::definitions::{
    Blob, CopiedTransferItem, File, FlightdeckData, FlightdeckMessage, FlightdeckObservation,
    RepositoryName, RepositorySyncState, TransferItem,
};
use chrono::{DateTime, TimeZone, Utc};
use log::ParseLevelError;
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

impl From<models::RepositorySyncState> for RepositorySyncState {
    fn from(repo: models::RepositorySyncState) -> Self {
        RepositorySyncState {
            repo_id: repo.repo_id.0,
            last_file_index: repo.last_file_index,
            last_blob_index: repo.last_blob_index,
            last_name_index: repo.last_name_index,
        }
    }
}

impl From<models::File> for File {
    fn from(file: models::File) -> Self {
        File {
            uid: file.uid.into(),
            path: file.path.0,
            blob_id: file.blob_id.map(|b| b.0),
            valid_from: datetime_to_timestamp(&file.valid_from),
        }
    }
}

impl From<models::Blob> for Blob {
    fn from(blob: models::Blob) -> Self {
        Blob {
            uid: blob.uid.into(),
            repo_id: blob.repo_id.0,
            blob_id: blob.blob_id.0,
            blob_size: blob.blob_size,
            has_blob: blob.has_blob,
            path: blob.path.map(|p| p.0),
            valid_from: datetime_to_timestamp(&blob.valid_from),
        }
    }
}

impl From<models::RepositoryName> for RepositoryName {
    fn from(repo_name: models::RepositoryName) -> Self {
        RepositoryName {
            uid: repo_name.uid.into(),
            repo_id: repo_name.repo_id.0,
            name: repo_name.name,
            valid_from: datetime_to_timestamp(&repo_name.valid_from),
        }
    }
}

impl From<RepositorySyncState> for models::RepositorySyncState {
    fn from(repo: RepositorySyncState) -> Self {
        models::RepositorySyncState {
            repo_id: RepoID(repo.repo_id),
            last_file_index: repo.last_file_index,
            last_blob_index: repo.last_blob_index,
            last_name_index: repo.last_name_index,
        }
    }
}

impl From<File> for models::File {
    fn from(file: File) -> Self {
        models::File {
            uid: file.uid.into(),
            path: models::Path(file.path),
            blob_id: file.blob_id.map(BlobID),
            valid_from: timestamp_to_datetime(&file.valid_from),
        }
    }
}

impl From<Blob> for models::Blob {
    fn from(blob: Blob) -> Self {
        models::Blob {
            uid: blob.uid.into(),
            repo_id: RepoID(blob.repo_id),
            blob_id: BlobID(blob.blob_id),
            blob_size: blob.blob_size,
            has_blob: blob.has_blob,
            path: blob.path.map(models::Path),
            valid_from: timestamp_to_datetime(&blob.valid_from),
        }
    }
}

impl From<RepositoryName> for models::RepositoryName {
    fn from(repo_name: RepositoryName) -> Self {
        models::RepositoryName {
            uid: repo_name.uid.into(),
            repo_id: RepoID(repo_name.repo_id),
            name: repo_name.name,
            valid_from: timestamp_to_datetime(&repo_name.valid_from),
        }
    }
}

impl From<TransferItem> for models::BlobTransferItem {
    fn from(i: TransferItem) -> Self {
        Self {
            transfer_id: i.transfer_id,
            blob_id: BlobID(i.blob_id),
            blob_size: i.blob_size,
            path: models::Path(i.path),
        }
    }
}

impl From<models::BlobTransferItem> for TransferItem {
    fn from(i: models::BlobTransferItem) -> Self {
        Self {
            transfer_id: i.transfer_id,
            blob_id: i.blob_id.0,
            blob_size: i.blob_size,
            path: i.path.0,
        }
    }
}

impl From<CopiedTransferItem> for models::CopiedTransferItem {
    fn from(i: CopiedTransferItem) -> Self {
        Self {
            transfer_id: i.transfer_id,
            blob_id: BlobID(i.blob_id),
            blob_size: i.blob_size,
            path: models::Path(i.path),
        }
    }
}

impl From<models::CopiedTransferItem> for CopiedTransferItem {
    fn from(i: models::CopiedTransferItem) -> Self {
        Self {
            transfer_id: i.transfer_id,
            blob_id: i.blob_id.0,
            blob_size: i.blob_size,
            path: i.path.0,
        }
    }
}

impl TryFrom<FlightdeckMessage> for flightdeck::observation::Message {
    type Error = ParseLevelError;
    fn try_from(m: FlightdeckMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            level: log::Level::from_str(&m.level)?,
            observation: m.observation.unwrap().into(),
        })
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

#[allow(clippy::fallible_impl_from)]
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
