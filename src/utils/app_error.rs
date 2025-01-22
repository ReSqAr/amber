use thiserror::Error;
use tonic::Status;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("tonic error: {0}")]
    Status(#[from] Status),
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
}

// Implement conversion from AppError to Status
impl From<AppError> for Status {
    fn from(error: AppError) -> Self {
        match error {
            AppError::Status(e) => e,
            _ => Status::from_error(Box::new(error)),
        }
    }
}
