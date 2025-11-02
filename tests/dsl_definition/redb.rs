use anyhow::{Context, anyhow};
use redb::{Database, TableDefinition};
use std::path::Path;

pub(crate) async fn run_action(repo_path: &Path, action: String) -> Result<(), anyhow::Error> {
    let redb_path = repo_path.join(".amb").join("virtual_filesystem.redb");
    match action.as_str() {
        "clear_latest_filesystem" => {
            let redb_path = redb_path.clone();
            tokio::task::spawn_blocking(move || {
                if let Some(parent) = redb_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                let db = if redb_path.exists() {
                    Database::open(&redb_path)?
                } else {
                    Database::create(&redb_path)?
                };

                let txn = db.begin_write()?;
                {
                    let mut path_table = txn.open_table(FILESYSTEM_BY_PATH_TABLE)?;
                    path_table.retain(|_, _| false)?;
                    let mut blob_table = txn.open_table(FILESYSTEM_BY_BLOB_TABLE)?;
                    blob_table.retain(|_, _| false)?;
                }
                txn.commit()?;
                Ok::<(), anyhow::Error>(())
            })
            .await
            .context("redb action failed")??;
            Ok(())
        }
        other => Err(anyhow!("Unknown redb action: {}", other)),
    }
}

const FILESYSTEM_BY_PATH_TABLE: TableDefinition<&str, Vec<u8>> =
    TableDefinition::new("filesystem_by_path");
const FILESYSTEM_BY_BLOB_TABLE: TableDefinition<&str, Vec<u8>> =
    TableDefinition::new("filesystem_by_blob");
