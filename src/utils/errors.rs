use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("blob {path} failed to be verified: expected: {expected} actual: {actual}")]
    UnexpectedBlobId {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("{connection_name} does not support {operation}")]
    UnsupportedRemote {
        connection_name: String,
        operation: String,
    },
    #[error("the repository is not initialised")]
    RepositoryNotInitialised(),
    #[error("the repository is already initialised")]
    RepositoryAlreadyInitialised(),
    #[error("connection {0} not found")]
    ConnectionNotFound(String),
    #[error("unable to parse '{raw}': {message}")]
    Parse { message: String, raw: String },
}

#[derive(Error, Debug)]
pub enum InternalError {
    #[error("tonic error: {0}")]
    Status(#[from] tonic::Status),
    #[error("tonic transport error: {0}")]
    Tonic(#[from] tonic::transport::Error),
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
    #[error("rclone error: exit code: {0}")]
    RClone(i32),
    #[error("ssh connection error: {0}")]
    Ssh(String),
}

// Implement conversion from AppError to Status
impl From<InternalError> for tonic::Status {
    fn from(error: InternalError) -> Self {
        match error {
            InternalError::Status(e) => e,
            _ => tonic::Status::from_error(Box::new(error)),
        }
    }
}
