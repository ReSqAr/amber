use duckdb::Connection;
use std::path::Path;

pub(crate) async fn run_sql(path: &Path, sql: String) -> Result<(), anyhow::Error> {
    let path = path.join(".amb/db.duckdb");
    let conn = Connection::open(&path)?;

    let result = conn.execute(sql.as_str(), [])?;
    println!("     > rows affected: {}", result);

    Ok(())
}
