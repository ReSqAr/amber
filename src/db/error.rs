use thiserror::Error;

#[derive(Error, Debug)]
pub enum DBError {
    #[error("an inconsistency was detected: {0}")]
    InconsistencyError(String),
    #[error("{0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("rocksdb error: {0}")]
    RocksDB(#[from] rocksdb::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] postcard::Error),
    #[error("send error: {0}")]
    SendError(String),
    #[error("database accessed after close")]
    AccessAfterDrop,
    #[error("scratch file {0} already exists")]
    ScratchFileExists(String),
    #[error("LogStore error {0}")]
    LogStore(#[from] crate::db::stores::log::Error),
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for DBError {
    fn from(e: tokio::sync::mpsc::error::SendError<T>) -> Self {
        DBError::SendError(format!("{:?}", e))
    }
}
