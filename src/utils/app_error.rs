use std::error::Error;
use std::fmt;
use tonic::Status;

#[derive(Debug)]
pub(crate) enum AppError {
    Status(Status),
    Sqlx(sqlx::Error),
    // You can add more error variants here if needed
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::Status(status) => write!(f, "Status error: {}", status),
            AppError::Sqlx(sqlx_error) => write!(f, "SQLx error: {}", sqlx_error),
        }
    }
}

impl Error for AppError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            AppError::Status(status) => Some(status),
            AppError::Sqlx(sqlx_error) => Some(sqlx_error),
        }
    }
}

// Implement From for Status
impl From<Status> for AppError {
    fn from(status: Status) -> Self {
        AppError::Status(status)
    }
}

// Implement From for sqlx::Error
impl From<sqlx::Error> for AppError {
    fn from(sqlx_error: sqlx::Error) -> Self {
        AppError::Sqlx(sqlx_error)
    }
}

// Implement conversion from AppError to Status
impl From<AppError> for Status {
    fn from(error: AppError) -> Self {
        Status::from_error(Box::new(error))
    }
}
