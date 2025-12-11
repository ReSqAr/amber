use crate::db::error::DBError;
use crate::db::models;
use crate::db::models::{BlobID, ConnectionName};
use crate::utils::walker;
use log::ParseLevelError;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("blob {path} failed to be verified: expected: {expected_hash} actual: {actual_hash}",
        expected_hash=expected.0,
        actual_hash=actual.0,
    )]
    UnexpectedBlobId {
        path: String,
        expected: BlobID,
        actual: BlobID,
    },
    #[error("connection {connection_name} does not support '{operation}'")]
    UnsupportedOperation {
        connection_name: String,
        operation: String,
    },
    #[error("the repository is not initialised")]
    RepositoryNotInitialised(),
    #[error("the repository is already initialised")]
    RepositoryAlreadyInitialised(),
    #[error("connection {name} not found", name=.0.0)]
    ConnectionNotFound(ConnectionName),
    #[error("file {0} is not part of the repository")]
    FileNotPartOfRepository(String),
    #[error("unable to parse '{raw}': {message}")]
    Parse { message: String, raw: String },
    #[error("source {0} does not exist")]
    SourceDoesNotExist(String),
    #[error("destination {0} does already exist")]
    DestinationDoesExist(String),
    #[error("destination {0} is a folder - expected it to be a file")]
    DestinationIsAFolder(String),
    #[error("move encountered errors")]
    MvErrors,
    #[error("remove encountered errors")]
    RmErrors,
    #[error("filesystem does neither support reflinks nor hardlinks")]
    RefAndHardlinksNotSupported,
    #[error("rclone is required but: {0}")]
    RCloneErr(String),
    #[error(
        "the transfer was incomplete: expected {expected_count} files but only {count} files were copied"
    )]
    IncompleteTransfer { count: u64, expected_count: u64 },
    #[error("please run fsck: blob {b} (backing file {p}) might be corrupted", b = blob_id.0, p = path.0)]
    CorruptionDetected {
        blob_id: models::BlobID,
        path: models::Path,
    },
    #[error("please run fsck: state of file {p} cannot be determined", p = path.0)]
    FileStateCannotBeDetermined { path: models::Path },
}

#[derive(Error, Debug)]
pub enum InternalError {
    #[error("tonic error: {0}")]
    Status(#[from] tonic::Status),
    #[error("tonic transport error: {0}")]
    Tonic(#[from] tonic::transport::Error),
    #[error("grpc error: {0}")]
    Grpc(String),
    #[error("parser level error: {0}")]
    ParseLevel(#[from] ParseLevelError),
    #[error("db error: {0}")]
    DBError(#[from] DBError),
    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
    #[error("stream error: {0}")]
    Stream(String),
    #[error("task execution failed: {0}")]
    TaskFailure(String),
    #[error("observation send error: {0}")]
    Send(String),
    #[error("fs walker error: {0}")]
    Walker(#[from] walker::Error),
    #[error("async error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("{0}")]
    App(#[from] AppError),
    #[error("ignore error: {0}")]
    Ignore(#[from] ignore::Error),
    #[error("rclone error: exit code: {0}")]
    RClone(i32),
    #[error("rclone configuration error: cannot extend global configuration with specific targets")]
    RCloneMixedConfig,
    #[error("ssh connection error: {0}")]
    Ssh(String),
    #[error("ssh error: {0}")]
    Russh(#[from] russh::Error),
    #[error("ssh credentials error: {0}")]
    RusshKeys(#[from] russh::keys::Error),
    #[error("serialisation error: {e} (object: {object})")]
    SerialisationError { object: String, e: String },
    #[error("unable to get exclusive lock on repository")]
    SharedAccess,
    #[error("invariant error: {0}")]
    InvariantError(String),
}

impl<T> From<SendError<T>> for InternalError {
    fn from(value: SendError<T>) -> Self {
        InternalError::Send(value.to_string())
    }
}

// Implement conversion from AppError to Status
impl From<InternalError> for tonic::Status {
    fn from(error: InternalError) -> Self {
        #[allow(clippy::wildcard_enum_match_arm)]
        match error {
            InternalError::Status(e) => e,
            _ => tonic::Status::from_error(Box::new(error)),
        }
    }
}
