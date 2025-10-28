use behemoth::StreamError;
use thiserror::Error;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum DBError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("DB migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("{0}")]
    StreamError(#[from] StreamError),
    #[error("{0}")]
    JoinError(#[from] JoinError),
}
