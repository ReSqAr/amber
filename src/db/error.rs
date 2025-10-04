use thiserror::Error;

#[derive(Error, Debug)]
pub enum DBError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("DB migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("redb database error: {0}")]
    RedbDatabase(#[from] redb::DatabaseError),
    #[error("redb transaction error: {0}")]
    RedbTransaction(#[from] redb::TransactionError),
    #[error("redb commit error: {0}")]
    RedbCommit(#[from] redb::CommitError),
    #[error("redb table error: {0}")]
    RedbTable(#[from] redb::TableError),
    #[error("redb storage error: {0}")]
    RedbStorage(#[from] redb::StorageError),
    #[error("redb error: {0}")]
    Redb(#[from] redb::Error),
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}
