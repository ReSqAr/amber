use thiserror::Error;
use tonic::Status;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("blob {path} failed to be verified: expected: {expected} actual: {actual}")]
    UnexpectedBlobId {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("{0} is not supported:")]
    UnsupportedRemote(String),
    #[error("the repository is not initialised")]
    RepositoryNotInitialised(),
    #[error("the repository is already initialised")]
    RepositoryAlreadyInitialised(),
    #[error("connection {0} not found")]
    ConnectionNotFound(String),
}

#[derive(Error, Debug)]
pub enum InternalError {
    #[error("tonic error: {0}")]
    Status(#[from] Status),
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("DB migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
    #[error("stream error: {0}")]
    Stream(String),
    #[error("{0}")]
    App(#[from] AppError),
}

// Implement conversion from AppError to Status
impl From<InternalError> for Status {
    fn from(error: InternalError) -> Self {
        match error {
            InternalError::Status(e) => e,
            _ => Status::from_error(Box::new(error)),
        }
    }
}
