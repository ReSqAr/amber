use crate::db::error::DBError;
use crate::db::redb_history::RedbHistoryStore;
use crate::db::redb_store::RedbStore;
use std::path::Path;

#[cfg_attr(not(test), allow(dead_code))]
pub async fn run_sql(repo_root: &Path, sql: &str) -> Result<u64, DBError> {
    // TODO: replace with enum - which 'table' to truncate - it's also not run_sql, but truncate sth
    let redb_path = repo_root.join(".amb/virtual_fs.redb");
    let store = RedbStore::open(&redb_path).await?;
    let history = RedbHistoryStore::new(store);
    history.apply_sql_mutation(sql).await?;

    Ok(0)
}
