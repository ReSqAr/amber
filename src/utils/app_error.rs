use thiserror::Error;
use tonic::Status;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("tonic error: {0}")]
    Status(#[from] Status),
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
}

// Implement conversion from AppError to Status
impl From<AppError> for Status {
    fn from(error: AppError) -> Self {
        Status::from_error(Box::new(error))
    }
}
