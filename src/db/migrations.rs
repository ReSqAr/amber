use crate::db::database::Pool;
use crate::db::error::DBError;

const MIGRATIONS: [&str; 3] = [
    include_str!("migrations/20250115000000_init.sql"),
    include_str!("migrations/20250117000000_views.sql"),
    include_str!("migrations/20250120000000_indices.sql"),
];

pub async fn run_migrations(con: &Pool) -> Result<(), DBError> {
    for migration in MIGRATIONS {
        for stmt in migration.split(";\n") {
            let sql = stmt.trim();
            if sql.is_empty() {
                continue;
            }
            con.with_conn(move |con| con.execute(sql, []).map_err(DBError::from))
                .await?;
        }
    }

    Ok(())
}
