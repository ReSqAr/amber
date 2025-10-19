use amber::VirtualFilesystemStore;
use amber::test_utils;
use std::path::Path;

pub(crate) async fn run_sql(path: &Path, sql: String) -> Result<(), anyhow::Error> {
    let rows = test_utils::run_sql(path, &sql).await?;
    println!("     > rows affected: {}", rows);

    Ok(())
}

pub(crate) async fn reset_virtual_filesystem(path: &Path) -> Result<(), anyhow::Error> {
    let db_path = path.join(".amb/virtual_fs.redb");
    let store = VirtualFilesystemStore::open(&db_path).await?;
    store.truncate().await?;
    println!("     > virtual filesystem reset");
    Ok(())
}
