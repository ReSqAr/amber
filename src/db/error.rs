use thiserror::Error;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum DBError {
    #[error("DB error: {0}")]
    Error(#[from] duckdb::Error),
    #[error("concurrency error: {0}")]
    JoinError(#[from] JoinError),
    #[error("other error: {0}")]
    Other(String),
}
