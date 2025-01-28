use thiserror::Error;
use tonic::Status;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("blob failed to be verified: {0}")]
    UnexpectedBlobId(String),
    #[error("unsupported remote: {0}")]
    UnsupportedRemote(String),
}

#[derive(Error, Debug)]
pub enum InternalError {
    #[error("tonic error: {0}")]
    Status(#[from] Status),
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
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
