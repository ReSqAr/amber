use crate::db::error::DBError;
use crate::db::models::{
    AvailableBlob, Blob, BlobAssociatedToFiles, BlobTransferItem, Connection as DbConnectionModel,
    ConnectionType, CopiedTransferItem, CurrentRepository, File, FileTransferItem, InsertBlob,
    InsertFile, InsertMaterialisation, InsertRepositoryName, Materialisation, MissingFile,
    Observation, ObservedBlob, Repository, RepositoryName, VirtualFile, VirtualFileState,
};
use crate::flightdeck::base::BaseObserver;
use crate::flightdeck::tracked::stream::Trackable;
use crate::utils::flow::{ExtFlow, Flow};
use crate::utils::stream::BoundedWaitChunksExt;
use duckdb::{Connection, OptionalExt, Params, Row, ToSql, params, params_from_iter};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, stream};
use log::debug;
use std::sync::{Arc, Mutex, mpsc};
use tokio::task::spawn_blocking;
use tokio::time;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

pub(crate) type DBOutputStream<'a, T> = BoxStream<'a, Result<T, DBError>>;

#[derive(Clone)]
pub struct Pool(Arc<Mutex<Connection>>);
impl Pool {
    pub fn new(database_url: &str) -> Result<Self, DBError> {
        let conn = Connection::open(database_url).map_err(DBError::from)?;
        let pool = Arc::new(Mutex::new(conn));
        Ok(Self(pool))
    }

    pub async fn with_conn<F, R>(&self, f: F) -> Result<R, DBError>
    where
        F: Send + 'static + FnOnce(&Connection) -> Result<R, DBError>,
        R: Send + 'static,
    {
        let con = self.0.clone();
        spawn_blocking(move || {
            let con = {
                let guard = con.lock().expect("mutex poisoned");
                guard.try_clone().expect("failed to clone DB connection")
            };
            f(&con)
        })
        .await
        .map_err(DBError::from)?
        .map_err(DBError::from)
    }
}

#[derive(Clone)]
pub struct Database {
    pub(crate) pool: Pool,
    pub(crate) temp_table_max_rows: usize,
}

impl Database {
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            temp_table_max_rows: 100_000,
        }
    }

    pub async fn clean(&self) -> Result<(), DBError> {
        self.pool
            .with_conn(move |conn| {
                conn.execute("DELETE FROM transfers;", [])?;
                Ok(())
            })
            .await
    }

    fn stream<'a, T, F>(
        &self,
        name: &str,
        sql: &'static str,
        params: impl Params + Send + Sync + 'static,
        map: F,
    ) -> DBOutputStream<'a, T>
    where
        T: Send + Unpin + 'static,
        F: Fn(&Row) -> Result<T, duckdb::Error> + Send + Sync + 'static,
    {
        let pool = self.pool.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<T, DBError>>(256);

        tokio::spawn(async move {
            let res = pool
                .with_conn(move |conn| {
                    let mut stmt = match conn.prepare(sql) {
                        Ok(stmt) => stmt,
                        Err(e) => {
                            log::error!("Database::stream: prepare failed: {e}");
                            return Err(DBError::from(e));
                        }
                    };

                    let iter = match stmt.query_map(params, |row| map(row)) {
                        Ok(iter) => iter,
                        Err(e) => {
                            log::error!("Database::stream: query_map failed: {e}");
                            return Err(DBError::from(e));
                        }
                    };
                    for item in iter {
                        match item {
                            Ok(v) => {
                                if let Err(e) = tx.blocking_send(Ok(v)) {
                                    log::error!("Database::stream: send failed: {e}");
                                    return Err(DBError::Other(e.to_string()));
                                }
                            }
                            Err(e) => {
                                if let Err(se) = tx.blocking_send(Err(DBError::from(e))) {
                                    log::error!("Database::stream: send error failed: {se}");
                                    return Err(DBError::Other(se.to_string()));
                                }
                                return Ok(());
                            }
                        }
                    }

                    Ok(())
                    // closes channel
                })
                .await;
            if let Err(e) = res {
                log::error!("Database::stream: failed to execute query: {e}");
            }
        });

        ReceiverStream::new(rx).track(name).boxed()
    }

    pub async fn get_or_create_current_repository(&self) -> Result<CurrentRepository, DBError> {
        let potential_new_repository_id = Uuid::new_v4().to_string();

        self.pool
            .with_conn(move |conn| {
                let mut stmt = conn.prepare(
                    "INSERT OR IGNORE INTO current_repository (id, repo_id) VALUES (1, ?);",
                )?;
                stmt.execute(params![&potential_new_repository_id])?;

                let repo = conn.query_row(
                    "SELECT id, repo_id FROM current_repository LIMIT 1;",
                    [],
                    |row| {
                        Ok(CurrentRepository {
                            repo_id: row.get::<_, String>(1)?,
                        })
                    },
                )?;
                Ok(repo)
            })
            .await
    }

    pub async fn lookup_current_repository_name(
        &self,
        repo_id: String,
    ) -> Result<Option<String>, DBError> {
        self.pool
            .with_conn(move |conn| {
                conn.query_row(
                    "SELECT name FROM latest_repository_names WHERE repo_id = ?;",
                    params![&repo_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(DBError::from)
            })
            .await
    }

    pub async fn add_files<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = InsertFile> + Unpin,
    {
        let (tx, rx) = mpsc::sync_channel::<InsertFile>(1024);

        let pool = self.pool.clone();
        let send_handle = tokio::spawn(async move {
            pool.with_conn(move |conn| {
                conn.execute_batch(
                    "CREATE TEMP TABLE tmp_files(
                    uuid TEXT,
                    path TEXT,
                    blob_id TEXT,
                    valid_from TIMESTAMP
                );",
                )?;

                {
                    let mut app = conn.appender("tmp_files")?;
                    for file in rx {
                        let params: [Box<dyn ToSql + Send + Sync>; 4] = [
                            Box::new(Uuid::new_v4().to_string()),
                            Box::new(file.path.clone()),
                            Box::new(file.blob_id.clone()),
                            Box::new(file.valid_from),
                        ];
                        app.append_row(params)?;
                    }
                }

                let inserted = conn.execute(
                    "INSERT INTO files (uuid, path, blob_id, valid_from)
                     SELECT uuid, path, blob_id, valid_from FROM tmp_files;",
                    [],
                )? as u64;

                let _ = conn.execute_batch("DROP TABLE tmp_files;");
                Ok(inserted)
            })
            .await
        });

        let mut attempted = 0u64;
        let mut s = s;
        while let Some(item) = s.next().await {
            attempted += 1;
            tx.send(item).map_err(|e| DBError::Other(e.to_string()))?;
        }

        drop(tx);

        let inserted = send_handle
            .await
            .map_err(|_| DBError::Other("appender sender task panicked".into()))??;

        debug!("files added: attempted={} inserted={}", attempted, inserted);
        Ok(inserted)
    }

    pub async fn add_blobs<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = InsertBlob> + Unpin,
    {
        let (tx, rx) = mpsc::sync_channel::<InsertBlob>(1024);

        let pool = self.pool.clone();
        let send_handle = tokio::spawn(async move {
            pool.with_conn(move |conn| {
                conn.execute_batch(
                    "CREATE TEMP TABLE tmp_blobs(
                    uuid TEXT,
                    repo_id TEXT,
                    blob_id TEXT,
                    blob_size BIGINT,
                    has_blob BOOLEAN,
                    path TEXT,
                    valid_from TIMESTAMP
                );",
                )?;

                {
                    let mut app = conn.appender("tmp_blobs")?;
                    for blob in rx {
                        let params: [Box<dyn ToSql + Send + Sync>; 7] = [
                            Box::new(Uuid::new_v4().to_string()),
                            Box::new(blob.repo_id.clone()),
                            Box::new(blob.blob_id.clone()),
                            Box::new(blob.blob_size),
                            Box::new(blob.has_blob),
                            Box::new(blob.path.clone()),
                            Box::new(blob.valid_from),
                        ];
                        app.append_row(params)?;
                    }
                }

                let inserted = conn
                    .execute(
                        "INSERT INTO blobs (uuid, repo_id, blob_id, blob_size, has_blob, path, valid_from)
                     SELECT uuid, repo_id, blob_id, blob_size, has_blob, path, valid_from FROM tmp_blobs;",
                        [],
                    )? as u64;

                let _ = conn.execute_batch("DROP TABLE tmp_blobs;");

                Ok(inserted)
            })
                .await
        });

        let mut attempted = 0u64;
        let mut s = s;
        while let Some(item) = s.next().await {
            attempted += 1;
            tx.send(item).map_err(|e| DBError::Other(e.to_string()))?;
        }

        drop(tx);

        let inserted = send_handle
            .await
            .map_err(|_| DBError::Other("appender sender task panicked".into()))??;

        debug!("blobs added: attempted={} inserted={}", attempted, inserted);
        Ok(inserted)
    }

    pub async fn observe_blobs<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = ObservedBlob> + Unpin,
    {
        let (tx, rx) = mpsc::sync_channel::<ObservedBlob>(1024);

        let pool = self.pool.clone();
        let send_handle = tokio::spawn(async move {
            pool.with_conn(move |conn| {
                conn.execute_batch(
                    "CREATE TEMP TABLE tmp_observed_blobs(
                    uuid TEXT,
                    repo_id TEXT,
                    has_blob BOOLEAN,
                    path TEXT,
                    valid_from TIMESTAMP
                );",
                )?;

                {
                    let mut app = conn.appender("tmp_observed_blobs")?;
                    for ob in rx {
                        let params: [Box<dyn ToSql + Send + Sync>; 5] = [
                            Box::new(Uuid::new_v4().to_string()),
                            Box::new(ob.repo_id.clone()),
                            Box::new(ob.has_blob),
                            Box::new(ob.path.clone()),
                            Box::new(ob.valid_from),
                        ];
                        app.append_row(params)?;
                    }
                }

                let inserted = conn.execute(
                    "
                INSERT INTO blobs (uuid, repo_id, blob_id, blob_size, has_blob, path, valid_from)
                SELECT
                    o.uuid,
                    o.repo_id,
                    lab.blob_id,
                    lab.blob_size,
                    o.has_blob,
                    o.path,
                    o.valid_from
                FROM tmp_observed_blobs o
                INNER JOIN latest_available_blobs lab USING (repo_id, path);
                ",
                    [],
                )? as u64;

                let _ = conn.execute_batch("DROP TABLE tmp_observed_blobs;");

                Ok(inserted)
            })
            .await
        });

        let mut attempted = 0u64;
        let mut s = s;
        while let Some(item) = s.next().await {
            attempted += 1;
            tx.send(item).map_err(|e| DBError::Other(e.to_string()))?;
        }

        drop(tx);

        let inserted = send_handle
            .await
            .map_err(|_| DBError::Other("appender sender task panicked".into()))??;

        debug!("blobs added: attempted={} inserted={}", attempted, inserted);
        Ok(inserted)
    }

    pub async fn add_repository_names<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = InsertRepositoryName> + Unpin,
    {
        let (tx, rx) = mpsc::sync_channel::<InsertRepositoryName>(1024);

        let pool = self.pool.clone();
        let send_handle = tokio::spawn(async move {
            pool.with_conn(move |conn| {
                conn.execute_batch(
                    "CREATE TEMP TABLE tmp_repository_names(
                    uuid TEXT,
                    repo_id TEXT,
                    name TEXT,
                    valid_from TIMESTAMP
                );",
                )?;

                {
                    let mut app = conn.appender("tmp_repository_names")?;
                    for rn in rx {
                        let params: [Box<dyn ToSql + Send + Sync>; 4] = [
                            Box::new(Uuid::new_v4().to_string()),
                            Box::new(rn.repo_id.clone()),
                            Box::new(rn.name.clone()),
                            Box::new(rn.valid_from),
                        ];
                        app.append_row(params)?;
                    }
                }

                let inserted = conn.execute(
                    "INSERT INTO repository_names (uuid, repo_id, name, valid_from)
                 SELECT uuid, repo_id, name, valid_from FROM tmp_repository_names;",
                    [],
                )? as u64;

                let _ = conn.execute_batch("DROP TABLE tmp_repository_names;");
                Ok(inserted)
            })
            .await
        });

        let mut attempted = 0u64;
        let mut s = s;
        while let Some(item) = s.next().await {
            attempted += 1;
            tx.send(item).map_err(|e| DBError::Other(e.to_string()))?;
        }

        drop(tx);

        let inserted = send_handle
            .await
            .map_err(|_| DBError::Other("appender sender task panicked".into()))??;

        debug!(
            "repository_names added: attempted={} inserted={}",
            attempted, inserted
        );
        Ok(inserted)
    }

    pub async fn select_repositories(&self) -> DBOutputStream<'static, Repository> {
        self.stream(
            "Database::select_repositories",
            "SELECT repo_id, last_file_index, last_blob_index, last_name_index FROM repositories",
            params_from_iter::<[u64; 0]>([]),
            |row| {
                Ok(Repository {
                    repo_id: row.get::<_, String>(0)?,
                    last_file_index: row.get::<_, i32>(1)?,
                    last_blob_index: row.get::<_, i32>(2)?,
                    last_name_index: row.get::<_, i32>(3)?,
                })
            },
        )
    }

    pub async fn select_files(&self, last_index: i32) -> DBOutputStream<'static, File> {
        self.stream(
            "Database::select_files",
            r#"
                SELECT uuid, path, blob_id, valid_from
                FROM files
                WHERE id > ?
            "#,
            [Box::new(last_index)],
            |row| {
                Ok(File {
                    uuid: row.get::<_, String>(0)?,
                    path: row.get::<_, String>(1)?,
                    blob_id: row.get::<_, Option<String>>(2)?,
                    valid_from: row.get(3)?,
                })
            },
        )
    }

    pub async fn select_blobs(&self, last_index: i32) -> DBOutputStream<'static, Blob> {
        self.stream(
            "Database::select_blobs",
            r#"
                SELECT uuid, repo_id, blob_id, blob_size, has_blob, path, valid_from
                FROM blobs
                WHERE id > ?
            "#,
            [Box::new(last_index)],
            |row| {
                Ok(Blob {
                    uuid: row.get::<_, String>(0)?,
                    repo_id: row.get::<_, String>(1)?,
                    blob_id: row.get::<_, String>(2)?,
                    blob_size: row.get::<_, i64>(3)?,
                    has_blob: row.get::<_, bool>(4)?,
                    path: row.get::<_, Option<String>>(5)?,
                    valid_from: row.get(6)?,
                })
            },
        )
    }

    pub async fn select_repository_names(
        &self,
        last_index: i32,
    ) -> DBOutputStream<'static, RepositoryName> {
        self.stream(
            "Database::select_repository_names",
            "
                SELECT uuid, repo_id, name, valid_from
                FROM repository_names
                WHERE id > ?
            ",
            [Box::new(last_index)],
            |row| {
                Ok(RepositoryName {
                    uuid: row.get::<_, String>(0)?,
                    repo_id: row.get::<_, String>(1)?,
                    name: row.get::<_, String>(2)?,
                    valid_from: row.get(3)?,
                })
            },
        )
    }

    pub async fn merge_repositories<S>(&self, s: S) -> Result<(), DBError>
    where
        S: Stream<Item = Repository> + Unpin + Send,
    {
        let (tx, rx) = mpsc::sync_channel::<Repository>(1024);

        let pool = self.pool.clone();
        let send_handle = tokio::spawn(async move {
            pool.with_conn(move |conn| {
                conn.execute_batch(
                    "CREATE TEMP TABLE tmp_repositories(
                    repo_id TEXT,
                    last_file_index INTEGER,
                    last_blob_index INTEGER,
                    last_name_index INTEGER
                );",
                )?;

                {
                    let mut app = conn.appender("tmp_repositories")?;
                    for repo in rx {
                        let params: [Box<dyn ToSql + Send + Sync>; 4] = [
                            Box::new(repo.repo_id.clone()),
                            Box::new(repo.last_file_index),
                            Box::new(repo.last_blob_index),
                            Box::new(repo.last_name_index),
                        ];
                        app.append_row(params)?;
                    }
                }

                let affected = conn.execute(
                    "
                INSERT INTO repositories (repo_id, last_file_index, last_blob_index, last_name_index)
                SELECT repo_id, last_file_index, last_blob_index, last_name_index
                FROM tmp_repositories
                ON CONFLICT (repo_id) DO UPDATE SET
                    last_file_index = GREATEST(repositories.last_file_index, excluded.last_file_index),
                    last_blob_index = GREATEST(repositories.last_blob_index, excluded.last_blob_index),
                    last_name_index = GREATEST(repositories.last_name_index, excluded.last_name_index);
                ",
                    [],
                )? as u64;

                let _ = conn.execute_batch("DROP TABLE tmp_repositories;");

                Ok(affected)
            })
                .await
        });

        let mut attempted: u64 = 0;
        let mut s = s;
        while let Some(item) = s.next().await {
            attempted += 1;
            tx.send(item).map_err(|e| DBError::Other(e.to_string()))?;
        }
        drop(tx);

        let inserted = send_handle
            .await
            .map_err(|_| DBError::Other("appender sender task panicked".into()))??;

        debug!(
            "repositories merged: attempted={} affected={}",
            attempted, inserted
        );
        Ok(())
    }

    pub async fn merge_files<S>(&self, s: S) -> Result<(), DBError>
    where
        S: Stream<Item = File> + Unpin + Send,
    {
        let (tx, rx) = mpsc::sync_channel::<File>(1024);

        let pool = self.pool.clone();
        let send_handle = tokio::spawn(async move {
            pool.with_conn(move |conn| {
                conn.execute_batch(
                    "CREATE TEMP TABLE tmp_files(
                    uuid TEXT,
                    path TEXT,
                    blob_id TEXT,
                    valid_from TIMESTAMP
                );",
                )?;

                {
                    let mut app = conn.appender("tmp_files")?;
                    for f in rx {
                        let params: [Box<dyn ToSql + Send + Sync>; 4] = [
                            Box::new(f.uuid.clone()),
                            Box::new(f.path.clone()),
                            Box::new(f.blob_id.clone()),
                            Box::new(f.valid_from),
                        ];
                        app.append_row(params)?;
                    }
                }

                let inserted = conn.execute(
                    "INSERT INTO files (uuid, path, blob_id, valid_from)
                    SELECT uuid, path, blob_id, valid_from FROM tmp_files
                    ON CONFLICT (uuid) DO NOTHING;",
                    [],
                )? as u64;

                let _ = conn.execute_batch("DROP TABLE tmp_files;");
                Ok(inserted)
            })
            .await
        });

        let mut attempted = 0u64;
        let mut s = s;
        while let Some(item) = s.next().await {
            attempted += 1;
            tx.send(item).map_err(|e| DBError::Other(e.to_string()))?;
        }

        drop(tx);

        let inserted = send_handle
            .await
            .map_err(|_| DBError::Other("appender sender task panicked".into()))??;

        debug!(
            "files merged: attempted={} inserted={}",
            attempted, inserted
        );
        Ok(())
    }

    pub async fn merge_blobs<S>(&self, s: S) -> Result<(), DBError>
    where
        S: Stream<Item = Blob> + Unpin + Send,
    {
        let (tx, rx) = mpsc::sync_channel::<Blob>(1024);

        let pool = self.pool.clone();
        let send_handle = tokio::spawn(async move {
            pool.with_conn(move |conn| {
                conn.execute_batch(
                    "CREATE TEMP TABLE tmp_blobs(
                    uuid TEXT,
                    repo_id TEXT,
                    blob_id TEXT,
                    blob_size BIGINT,
                    has_blob BOOLEAN,
                    path TEXT,
                    valid_from TIMESTAMP
                );",
                )?;

                {
                    let mut app = conn.appender("tmp_blobs")?;
                    for b in rx {
                        let params: [Box<dyn ToSql + Send + Sync>; 7] = [
                            Box::new(b.uuid.clone()),
                            Box::new(b.repo_id.clone()),
                            Box::new(b.blob_id.clone()),
                            Box::new(b.blob_size),
                            Box::new(b.has_blob),
                            Box::new(b.path.clone()),
                            Box::new(b.valid_from),
                        ];
                        app.append_row(params)?;
                    }
                }

                let inserted = conn
                    .execute(
                        "INSERT INTO blobs (uuid, repo_id, blob_id, blob_size, has_blob, path, valid_from)
                     SELECT uuid, repo_id, blob_id, blob_size, has_blob, path, valid_from FROM tmp_blobs
                     ON CONFLICT (uuid) DO NOTHING;",
                        [],
                    )? as u64;

                let _ = conn.execute_batch("DROP TABLE tmp_blobs;");

                Ok(inserted)
            })
                .await
        });

        let mut attempted = 0u64;
        let mut s = s;
        while let Some(item) = s.next().await {
            attempted += 1;
            tx.send(item).map_err(|e| DBError::Other(e.to_string()))?;
        }

        drop(tx);

        let inserted = send_handle
            .await
            .map_err(|_| DBError::Other("appender sender task panicked".into()))??;

        debug!(
            "blobs merged: attempted={} inserted={}",
            attempted, inserted
        );
        Ok(())
    }

    pub async fn merge_repository_names<S>(&self, s: S) -> Result<(), DBError>
    where
        S: Stream<Item = RepositoryName> + Unpin + Send,
    {
        let (tx, rx) = mpsc::sync_channel::<RepositoryName>(1024);

        let pool = self.pool.clone();
        let send_handle = tokio::spawn(async move {
            pool.with_conn(move |conn| {
                conn.execute_batch(
                    "CREATE TEMP TABLE tmp_repository_names(
                    uuid TEXT,
                    repo_id TEXT,
                    name TEXT,
                    valid_from TIMESTAMP
                );",
                )?;

                {
                    let mut app = conn.appender("tmp_repository_names")?;
                    for rn in rx {
                        let params: [Box<dyn ToSql + Send + Sync>; 4] = [
                            Box::new(rn.uuid.clone()),
                            Box::new(rn.repo_id.clone()),
                            Box::new(rn.name.clone()),
                            Box::new(rn.valid_from),
                        ];
                        app.append_row(params)?;
                    }
                }

                let inserted = conn.execute(
                    "INSERT INTO repository_names (uuid, repo_id, name, valid_from)
                    SELECT uuid, repo_id, name, valid_from FROM tmp_repository_names
                    ON CONFLICT(uuid) DO NOTHING;",
                    [],
                )? as u64;

                let _ = conn.execute_batch("DROP TABLE tmp_repository_names;");
                Ok(inserted)
            })
            .await
        });

        let mut attempted = 0u64;
        let mut s = s;
        while let Some(item) = s.next().await {
            attempted += 1;
            tx.send(item).map_err(|e| DBError::Other(e.to_string()))?;
        }

        drop(tx);

        let inserted = send_handle
            .await
            .map_err(|_| DBError::Other("appender sender task panicked".into()))??;

        debug!(
            "repository_names merged: attempted={} inserted={}",
            attempted, inserted
        );
        Ok(())
    }

    pub async fn lookup_repository(&self, repo_id: String) -> Result<Repository, DBError> {
        self.pool
            .with_conn(move |conn| {
                let sql = "
                SELECT COALESCE(r.repo_id, ?) as repo_id,
                       COALESCE(r.last_file_index, -1) as last_file_index,
                       COALESCE(r.last_blob_index, -1) as last_blob_index,
                       COALESCE(r.last_name_index, -1) as last_name_index
                FROM (SELECT NULL) n
                LEFT JOIN repositories r ON r.repo_id = ?
                LIMIT 1
            ";
                conn.query_row(sql, params![&repo_id, &repo_id], |row| {
                    Ok(Repository {
                        repo_id: row.get::<_, String>(0)?,
                        last_file_index: row.get::<_, i32>(1)?,
                        last_blob_index: row.get::<_, i32>(2)?,
                        last_name_index: row.get::<_, i32>(3)?,
                    })
                })
                .map_err(DBError::from)
            })
            .await
    }

    pub async fn update_last_indices(&self) -> Result<Repository, DBError> {
        self.pool.with_conn(move |conn| {
            let sql = "
                INSERT INTO repositories (repo_id, last_file_index, last_blob_index, last_name_index)
                SELECT
                    (SELECT repo_id FROM current_repository LIMIT 1),
                    (SELECT COALESCE(MAX(id), -1) FROM files),
                    (SELECT COALESCE(MAX(id), -1) FROM blobs),
                    (SELECT COALESCE(MAX(id), -1) FROM repository_names)
                ON CONFLICT(repo_id) DO UPDATE
                SET
                    last_file_index = GREATEST(excluded.last_file_index, repositories.last_file_index),
                    last_blob_index = GREATEST(excluded.last_blob_index, repositories.last_blob_index),
                    last_name_index = GREATEST(excluded.last_name_index, repositories.last_name_index)
                RETURNING repo_id, last_file_index, last_blob_index, last_name_index;
            ";
            conn.query_row(sql, [], |row| {
                Ok(Repository {
                    repo_id: row.get::<_, String>(0)?,
                    last_file_index: row.get::<_, i32>(1)?,
                    last_blob_index: row.get::<_, i32>(2)?,
                    last_name_index: row.get::<_, i32>(3)?,
                })
            }).map_err(DBError::from)
        }).await
    }

    pub async fn add_materialisations<S>(&self, s: S) -> Result<u64, DBError>
    where
        S: Stream<Item = InsertMaterialisation> + Unpin,
    {
        let (tx, rx) = mpsc::sync_channel::<InsertMaterialisation>(1024);

        let pool = self.pool.clone();
        let send_handle = tokio::spawn(async move {
            pool.with_conn(move |conn| {
                conn.execute_batch(
                    "CREATE TEMP TABLE tmp_materialisations(
                    path TEXT,
                    blob_id TEXT,
                    valid_from TIMESTAMP
                );",
                )?;

                {
                    let mut app = conn.appender("tmp_materialisations")?;
                    for m in rx {
                        let params: [Box<dyn ToSql + Send + Sync>; 3] = [
                            Box::new(m.path.clone()),
                            Box::new(m.blob_id.clone()),
                            Box::new(m.valid_from),
                        ];
                        app.append_row(params)?;
                    }
                }

                let inserted = conn.execute(
                    "INSERT INTO materialisations (path, blob_id, valid_from)
                     SELECT path, blob_id, valid_from FROM tmp_materialisations;",
                    [],
                )? as u64;

                let _ = conn.execute_batch("DROP TABLE tmp_materialisations;");

                Ok(inserted)
            })
            .await
        });

        let mut attempted = 0u64;
        let mut s = s;
        while let Some(item) = s.next().await {
            attempted += 1;
            tx.send(item).map_err(|e| DBError::Other(e.to_string()))?;
        }

        drop(tx);

        let inserted = send_handle
            .await
            .map_err(|_| DBError::Other("appender sender task panicked".into()))??;

        debug!(
            "materialisations added: attempted={} inserted={}",
            attempted, inserted
        );
        Ok(inserted)
    }

    pub async fn lookup_last_materialisation(
        &self,
        path: String,
    ) -> Result<Option<Materialisation>, DBError> {
        self.pool
            .with_conn(move |conn| {
                conn.query_row(
                    "SELECT path, blob_id FROM latest_materialisations WHERE path = ?;",
                    params![&path],
                    |row| {
                        Ok(Materialisation {
                            path: row.get::<_, String>(0)?,
                            blob_id: row.get::<_, Option<String>>(1)?,
                        })
                    },
                )
                .optional()
                .map_err(DBError::from)
            })
            .await
    }

    pub async fn truncate_virtual_filesystem(&self) -> Result<(), DBError> {
        self.pool
            .with_conn(move |conn| {
                conn.execute("DELETE FROM virtual_filesystem;", [])?;
                Ok(())
            })
            .await
    }

    pub async fn refresh_virtual_filesystem(&self) -> Result<(), DBError> {
        const QUERY: &str = r#"
        INSERT INTO virtual_filesystem (
            path,
            materialisation_last_blob_id,
            target_blob_id,
            target_blob_size,
            local_has_target_blob
        )
        WITH
            locally_available_blobs AS (
                SELECT
                    ab.blob_id,
                    ab.blob_size
                FROM latest_available_blobs ab
                INNER JOIN current_repository cr ON ab.repo_id = cr.repo_id
            ),
            latest_filesystem_files_with_materialisation_and_availability AS (
                SELECT
                    f.path,
                    m.blob_id AS materialisation_last_blob_id,
                    CASE
                        WHEN a.blob_id IS NOT NULL THEN TRUE
                        ELSE FALSE
                    END AS local_has_blob,
                    f.blob_id,
                    a.blob_size
                FROM latest_filesystem_files f
                LEFT JOIN locally_available_blobs a ON f.blob_id = a.blob_id
                LEFT JOIN latest_materialisations m ON m.path = f.path

                UNION ALL

                SELECT -- files which are supposed to be deleted but still have a materialisation
                    m.path AS path,
                    m.blob_id AS materialisation_last_blob_id,
                    FALSE AS local_has_blob,
                    NULL AS blob_id,
                    NULL AS blob_size
                FROM latest_materialisations m
                LEFT JOIN latest_filesystem_files f ON f.path = m.path
                WHERE f.blob_id IS NULL
            )
        SELECT
            a.path,
            a.materialisation_last_blob_id,
            a.blob_id AS target_blob_id,
            a.blob_size AS target_blob_size,
            a.local_has_blob AS local_has_target_blob
        FROM latest_filesystem_files_with_materialisation_and_availability a
        LEFT JOIN virtual_filesystem vfs ON vfs.path = a.path
        WHERE
              a.materialisation_last_blob_id IS DISTINCT FROM vfs.materialisation_last_blob_id
           OR a.blob_id IS DISTINCT FROM vfs.target_blob_id
           OR a.blob_size IS DISTINCT FROM vfs.target_blob_size
           OR a.local_has_blob IS DISTINCT FROM vfs.local_has_target_blob

        UNION ALL

        SELECT -- files not tracked anymore, deleted in files & no materialisation
            vfs.path AS path,
            NULL AS materialisation_last_blob_id,
            NULL AS target_blob_id,
            NULL AS target_blob_size,
            FALSE AS local_has_target_blob
        FROM virtual_filesystem vfs
        LEFT JOIN latest_filesystem_files_with_materialisation_and_availability a
               ON a.path = vfs.path
        WHERE
            a.path IS NULL
            AND (
                  NULL IS DISTINCT FROM vfs.materialisation_last_blob_id
               OR NULL IS DISTINCT FROM vfs.target_blob_id
               OR NULL IS DISTINCT FROM vfs.target_blob_size
               OR FALSE IS DISTINCT FROM vfs.local_has_target_blob
            )

        ON CONFLICT (path) DO UPDATE SET
            materialisation_last_blob_id = excluded.materialisation_last_blob_id,
            target_blob_id               = excluded.target_blob_id,
            target_blob_size             = excluded.target_blob_size,
            local_has_target_blob        = excluded.local_has_target_blob,
            fs_last_seen_id              = NULL,
            fs_last_seen_dttm            = NULL,
            fs_last_modified_dttm        = NULL,
            fs_last_size                 = NULL,
            check_last_dttm              = NULL,
            check_last_hash              = NULL
        ;
    "#;

        self.pool
            .with_conn(move |conn| {
                let changed = conn.execute(QUERY, [])?;
                log::debug!("refresh_virtual_filesystem: rows affected={}", changed);
                Ok(())
            })
            .await
    }

    pub async fn cleanup_virtual_filesystem(&self, last_seen_id: i64) -> Result<(), DBError> {
        let query = "
        DELETE FROM virtual_filesystem
        WHERE fs_last_seen_id IS DISTINCT FROM ? AND target_blob_id IS NULL AND materialisation_last_blob_id IS NULL;
        ";

        self.pool
            .with_conn(move |conn| {
                let changed = conn.execute(query, params![last_seen_id])?;
                debug!("cleanup_virtual_filesystem: rows affected={}", changed);
                Ok(())
            })
            .await
    }

    pub async fn select_missing_files_on_virtual_filesystem(
        &self,
        last_seen_id: i64,
    ) -> DBOutputStream<'static, MissingFile> {
        self.stream(
            "Database::select_missing_files_on_virtual_filesystem",
            "
                    SELECT
                        path,
                        target_blob_id,
                        local_has_target_blob
                    FROM virtual_filesystem
                    WHERE (fs_last_seen_id != ? OR fs_last_seen_id IS NULL) AND target_blob_id IS NOT NULL
                ;",
            [last_seen_id],
            |row| {
                Ok(MissingFile {
                    path: row.get::<_, String>(0)?,
                    target_blob_id: row.get::<_, String>(1)?,
                    local_has_target_blob: row.get::<_, bool>(2)?,
                })
            },
        )
    }

    pub async fn add_virtual_filesystem_observations(
        &self,
        input_stream: impl Stream<Item = Flow<Observation>> + Unpin + Send + 'static,
    ) -> impl Stream<Item = ExtFlow<Result<Vec<VirtualFile>, DBError>>> + Unpin + Send + 'static
    {
        let s = self.clone();

        input_stream
            .bounded_wait_chunks(self.temp_table_max_rows, time::Duration::from_millis(1))
            .then(move |chunk: Vec<Flow<Observation>>| {
                let s = s.clone();
                Box::pin(async move {
                    let mut obs = BaseObserver::without_id("add_virtual_filesystem_observations::inner");
                    obs.observe_length(log::Level::Debug, chunk.len() as u64);
                    let shutting_down = chunk.iter().any(|m| matches!(m, Flow::Shutdown));
                    let observations: Vec<_> = chunk
                        .iter()
                        .filter_map(|m| match m {
                            Flow::Data(observation) => Some(observation.clone()),
                            Flow::Shutdown => None,
                        })
                        .collect();

                    if observations.is_empty() {
                        return if shutting_down {
                            ExtFlow::Shutdown(Ok(vec![]))
                        } else {
                            ExtFlow::Data(Ok(vec![]))
                        };
                    }

                    let result: Result<Vec<VirtualFile>, DBError> = s.pool.with_conn(move |conn| {
                        conn.execute_batch(
                            r#"
                        CREATE TEMP TABLE tmp_seen (
                            path TEXT,
                            fs_last_seen_id BIGINT,
                            fs_last_seen_dttm BIGINT,
                            fs_last_modified_dttm BIGINT,
                            fs_last_size BIGINT
                        );

                        CREATE TEMP TABLE tmp_check (
                            path TEXT,
                            check_last_dttm BIGINT,
                            check_last_hash TEXT
                        );
                        "#,
                        )?;

                        {
                            let mut app_seen = conn.appender("tmp_seen")?;
                            let mut app_check = conn.appender("tmp_check")?;

                            for o in &observations {
                                match o {
                                    Observation::FileSeen(fs) => {
                                        let params: [Box<dyn ToSql + Send + Sync>; 5] = [
                                            Box::new(fs.path.clone()),
                                            Box::new(fs.seen_id),
                                            Box::new(fs.seen_dttm),
                                            Box::new(fs.last_modified_dttm),
                                            Box::new(fs.size),
                                        ];
                                        app_seen.append_row(params)?;
                                    }
                                    Observation::FileCheck(fc) => {
                                        let params: [Box<dyn ToSql + Send + Sync>; 3] = [
                                            Box::new(fc.path.clone()),
                                            Box::new(fc.check_dttm),
                                            Box::new(fc.hash.clone()),
                                        ];
                                        app_check.append_row(params)?;
                                    }
                                }
                            }

                            drop(app_seen);
                            drop(app_check);
                        }

                        let sql = r#"
                        INSERT INTO virtual_filesystem (
                            path,
                            fs_last_seen_id,
                            fs_last_seen_dttm,
                            fs_last_modified_dttm,
                            fs_last_size,
                            check_last_dttm,
                            check_last_hash
                        )
                        SELECT
                            path,
                            fs_last_seen_id,
                            fs_last_seen_dttm,
                            fs_last_modified_dttm,
                            fs_last_size,
                            check_last_dttm,
                            check_last_hash
                        FROM tmp_seen s
                            FULL OUTER JOIN tmp_check c USING (path)
                        ON CONFLICT(path) DO UPDATE SET
                            fs_last_seen_id       = COALESCE(excluded.fs_last_seen_id,       virtual_filesystem.fs_last_seen_id),
                            fs_last_seen_dttm     = COALESCE(excluded.fs_last_seen_dttm,     virtual_filesystem.fs_last_seen_dttm),
                            fs_last_modified_dttm = COALESCE(excluded.fs_last_modified_dttm, virtual_filesystem.fs_last_modified_dttm),
                            fs_last_size          = COALESCE(excluded.fs_last_size,          virtual_filesystem.fs_last_size),
                            check_last_dttm       = COALESCE(excluded.check_last_dttm,       virtual_filesystem.check_last_dttm),
                            check_last_hash       = COALESCE(excluded.check_last_hash,       virtual_filesystem.check_last_hash)
                        RETURNING
                            path,
                            materialisation_last_blob_id,
                            target_blob_id,
                            local_has_target_blob,
                            CASE
                                WHEN target_blob_id IS NULL AND materialisation_last_blob_id IS NULL THEN 'new'
                                WHEN fs_last_modified_dttm <= check_last_dttm THEN (
                                    CASE
                                        WHEN check_last_hash IS DISTINCT FROM target_blob_id THEN (
                                            CASE
                                                WHEN check_last_hash = materialisation_last_blob_id THEN 'outdated'
                                                ELSE 'altered'
                                            END
                                        )
                                        WHEN target_blob_size IS NOT NULL AND fs_last_size IS NOT NULL AND fs_last_size IS DISTINCT FROM target_blob_size THEN 'needs_check'
                                        WHEN check_last_hash IS DISTINCT FROM materialisation_last_blob_id THEN 'ok_materialisation_missing'
                                        ELSE 'ok'
                                    END
                                )
                                ELSE 'needs_check'
                            END AS state
                        ;
                        "#;

                        let rows = conn
                            .prepare(sql)?
                            .query_map([], |row| {
                                Ok(VirtualFile {
                                    path: row.get::<_, String>(0)?,
                                    materialisation_last_blob_id: row.get::<_, Option<String>>(1)?,
                                    target_blob_id: row.get::<_, Option<String>>(2)?,
                                    local_has_target_blob: row.get::<_, bool>(3)?,
                                    state: row.get::<_, VirtualFileState>(4)?,
                                })
                            })?
                            .collect::<Result<Vec<_>, _>>()?;

                        let _ = conn.execute_batch(
                            r#"
                        DROP TABLE IF EXISTS tmp_seen;
                        DROP TABLE IF EXISTS tmp_check;
                        "#,
                        );

                        Ok(rows)
                    }).await;

                    obs.observe_termination(log::Level::Debug, "done");
                    if shutting_down {
                        ExtFlow::Shutdown(result)
                    } else {
                        ExtFlow::Data(result)
                    }
                })
            })
            .boxed()
            .track("Database::add_virtual_filesystem_observations")
    }

    pub async fn add_connection(&self, connection: &DbConnectionModel) -> Result<(), DBError> {
        let sql = "
            INSERT INTO connections (name, connection_type, parameter)
            VALUES (?, ?, ?);
        ";
        let name = connection.name.clone();
        let ctype = connection.connection_type.clone();
        let param = connection.parameter.clone();

        self.pool
            .with_conn(move |conn| {
                let mut stmt = conn.prepare(sql)?;
                stmt.execute(params![&name, &ctype, &param])?;
                Ok(())
            })
            .await
    }

    pub async fn connection_by_name(
        &self,
        name: &str,
    ) -> Result<Option<DbConnectionModel>, DBError> {
        let name = name.to_string();
        self.pool
            .with_conn(move |conn| {
                conn.query_row(
                    "
                SELECT name, connection_type, parameter
                FROM connections
                WHERE name = ?
                ",
                    params![&name],
                    |row| {
                        Ok(DbConnectionModel {
                            name: row.get::<_, String>(0)?,
                            connection_type: row.get::<_, ConnectionType>(1)?,
                            parameter: row.get::<_, String>(2)?,
                        })
                    },
                )
                .optional()
                .map_err(DBError::from)
            })
            .await
    }

    pub async fn list_all_connections(&self) -> Result<Vec<DbConnectionModel>, DBError> {
        self.pool
            .with_conn(move |conn| {
                let mut stmt = conn.prepare(
                    "
                SELECT name, connection_type, parameter
                FROM connections
                ",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok(DbConnectionModel {
                            name: row.get::<_, String>(0)?,
                            connection_type: row.get::<_, ConnectionType>(1)?,
                            parameter: row.get::<_, String>(2)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await
    }

    pub(crate) fn available_blobs(
        &self,
        repo_id: String,
    ) -> impl Stream<Item = Result<AvailableBlob, DBError>> + Unpin + Send + Sized + 'static {
        self.stream(
            "Database::available_blobs",
            "
                SELECT repo_id, blob_id, blob_size, path
                FROM latest_available_blobs
                WHERE repo_id = ?",
            [Box::new(repo_id)],
            |row| {
                Ok(AvailableBlob {
                    repo_id: row.get::<_, String>(0)?,
                    blob_id: row.get::<_, String>(1)?,
                    blob_size: row.get::<_, i64>(2)?,
                    path: row.get::<_, Option<String>>(3)?,
                })
            },
        )
    }

    pub(crate) fn missing_blobs(
        &self,
        repo_id: String,
    ) -> impl Stream<Item = Result<BlobAssociatedToFiles, DBError>> + Unpin + Send + Sized + 'static
    {
        self.stream(
            "Database::missing_blobs",
            "
                WITH blobs_with_repository_names AS (
                    SELECT
                        blob_id,
                        json_group_array(COALESCE(rn.name, ab.repo_id)) AS repository_names,
                        MAX(CASE WHEN ab.repo_id = ? THEN 1 ELSE 0 END) AS is_available
                    FROM latest_available_blobs ab
                        LEFT JOIN latest_repository_names rn ON ab.repo_id = rn.repo_id
                    GROUP BY
                        blob_id
                ), missing_blobs_with_paths AS (
                    SELECT
                        fs.blob_id,
                        brn.repository_names,
                        json_group_array(fs.path) AS paths
                    FROM latest_filesystem_files fs
                        LEFT JOIN blobs_with_repository_names brn ON fs.blob_id = brn.blob_id
                    WHERE NOT COALESCE(brn.is_available, FALSE)
                    GROUP BY fs.blob_id, brn.repository_names
                )
                SELECT
                    blob_id,
                    paths,
                    COALESCE(repository_names, '[]') AS repositories_with_blob
                FROM missing_blobs_with_paths
                ORDER BY paths;
            ",
            [Box::new(repo_id)],
            |row| {
                let blob_id: String = row.get(0)?;
                let paths_json: String = row.get(1)?;
                let repos_json: String = row.get(2)?;

                let paths: Vec<String> = serde_json::from_str(&paths_json).map_err(|e| {
                    duckdb::Error::FromSqlConversionFailure(
                        0,
                        duckdb::types::Type::Text,
                        Box::new(e),
                    )
                })?;
                let repositories_with_blob: Vec<String> = serde_json::from_str(&repos_json)
                    .map_err(|e| {
                        duckdb::Error::FromSqlConversionFailure(
                            0,
                            duckdb::types::Type::Text,
                            Box::new(e),
                        )
                    })?;

                Ok(BlobAssociatedToFiles {
                    blob_id,
                    paths,
                    repositories_with_blob,
                })
            },
        )
    }

    pub(crate) async fn populate_missing_blobs_for_transfer(
        &self,
        transfer_id: u32,
        remote_repo_id: String,
        paths: Vec<String>,
    ) -> DBOutputStream<'static, BlobTransferItem> {
        self.stream(
            "Database::populate_missing_blobs_for_transfer",
            "
            INSERT INTO transfers (transfer_id, blob_id, blob_size, path)
            WITH
                local_blobs AS (
                    SELECT blob_id
                    FROM latest_available_blobs
                    INNER JOIN current_repository USING (repo_id)
                ),
                remote_blobs AS (
                    SELECT
                        blob_id,
                        blob_size
                    FROM latest_available_blobs
                    WHERE repo_id = ?
                ),
                path_selectors AS (
                        SELECT value::JSON ->> '$' AS path_selector
                        FROM json_each(json(?))
                ),
                selected_files AS (
                    SELECT f.blob_id, f.path
                    FROM latest_filesystem_files f
                    INNER JOIN path_selectors p ON f.path LIKE p.path_selector || '%'
                    UNION ALL
                    SELECT f.blob_id, f.path
                    FROM latest_filesystem_files f
                    WHERE ? = '[]'
                ),
                missing_blob_ids AS (
                    SELECT DISTINCT f.blob_id
                    FROM selected_files f
                    LEFT JOIN local_blobs lb ON f.blob_id = lb.blob_id
                    WHERE lb.blob_id IS NULL
                )
            SELECT
                ? AS transfer_id,
                m.blob_id,
                rb.blob_size,
                CASE
                    WHEN length(m.blob_id) > 6
                        THEN substr(m.blob_id, 1, 2) || '/' || substr(m.blob_id, 3, 2) || '/' || substr(m.blob_id, 5)
                    ELSE  m.blob_id
                END AS path
            FROM missing_blob_ids m
            INNER JOIN remote_blobs rb ON m.blob_id = rb.blob_id
            RETURNING transfer_id, blob_id, path;",
            [
                Box::new(remote_repo_id) as Box<dyn ToSql + Send + Sync>,
                Box::new(serde_json::to_string(&paths).unwrap()),
                Box::new(serde_json::to_string(&paths).unwrap()),
                Box::new(transfer_id),
            ],
            |row| {
                Ok(BlobTransferItem {
                    transfer_id: row.get::<_, i64>(0)? as u32,
                    blob_id: row.get::<_, String>(1)?,
                    path: row.get::<_, String>(2)?,
                })
            },
        )
    }

    pub(crate) async fn select_blobs_transfer(
        &self,
        input_stream: impl Stream<Item = CopiedTransferItem> + Unpin + Send + 'static,
    ) -> DBOutputStream<'static, BlobTransferItem> {
        let s = self.clone();

        input_stream
            .bounded_wait_chunks(self.temp_table_max_rows, time::Duration::from_millis(1))
            .then(move |chunk: Vec<CopiedTransferItem>| {
                let s = s.clone();
                Box::pin(async move {
                    if chunk.is_empty() {
                        return stream::iter(vec![]).boxed();
                    }

                    let rows: Result<Vec<BlobTransferItem>, DBError> = s
                        .pool
                        .with_conn(move |conn| {
                            conn.execute_batch(
                                "CREATE TEMP TABLE tmp_transfers_input(
                                transfer_id BIGINT,
                                path TEXT
                            );",
                            )?;

                            {
                                let mut app = conn.appender("tmp_transfers_input")?;
                                for r in &chunk {
                                    let params: [Box<dyn ToSql + Send + Sync>; 2] =
                                        [Box::new(r.transfer_id as i64), Box::new(r.path.clone())];
                                    app.append_row(params)?;
                                }
                            }

                            let mut stmt = conn.prepare(
                                "
                            SELECT transfer_id, blob_id, path
                            FROM transfers
                            INNER JOIN tmp_transfers_input USING (transfer_id, path);
                            ",
                            )?;
                            let rows = stmt
                                .query_map([], |row| {
                                    Ok(BlobTransferItem {
                                        transfer_id: row.get::<_, i64>(0)? as u32,
                                        blob_id: row.get::<_, String>(1)?,
                                        path: row.get::<_, String>(2)?,
                                    })
                                })?
                                .collect::<Result<Vec<_>, _>>()?;

                            let _ = conn.execute_batch("DROP TABLE tmp_transfers_input;");
                            Ok(rows)
                        })
                        .await;

                    stream::iter(match rows {
                        Ok(v) => v.into_iter().map(Ok).collect::<Vec<_>>(),
                        Err(e) => vec![Err(e)],
                    })
                    .boxed()
                })
            })
            .flatten()
            .boxed()
            .track("Database::select_blobs_transfer")
            .boxed()
    }

    pub(crate) async fn populate_missing_files_for_transfer(
        &self,
        transfer_id: u32,
        local_repo_id: String,
        remote_repo_id: String,
        paths: Vec<String>,
    ) -> DBOutputStream<'static, FileTransferItem> {
        self.stream(
            "Database::populate_missing_files_for_transfer",
            "
            INSERT INTO transfers (transfer_id, blob_id, blob_size, path)
            WITH
                local_blobs AS (
                    SELECT blob_id
                    FROM latest_available_blobs
                    WHERE repo_id = ?
                ),
                remote_blobs AS (
                    SELECT
                        blob_id,
                        blob_size
                    FROM latest_available_blobs
                    WHERE repo_id = ?
                ),
                path_selectors AS (
                    SELECT value::JSON ->> '$' AS path_selector
                    FROM json_each(json(?))
                ),
                selected_files AS (
                    SELECT f.blob_id, f.path
                    FROM latest_filesystem_files f
                    INNER JOIN path_selectors p ON f.path LIKE p.path_selector || '%'
                    UNION ALL
                    SELECT f.blob_id, f.path
                    FROM latest_filesystem_files f
                    WHERE ? = '[]'
                ),
                missing_file_blob_ids AS (
                    SELECT
                        f.blob_id,
                        MIN(f.path) as path
                    FROM selected_files f
                    LEFT JOIN local_blobs lb ON f.blob_id = lb.blob_id
                    WHERE lb.blob_id IS NULL
                    GROUP BY f.blob_id
                )
            SELECT
                ? AS transfer_id,
                m.blob_id,
                rb.blob_size,
                m.path
            FROM missing_file_blob_ids m
            INNER JOIN remote_blobs rb ON m.blob_id = rb.blob_id
            RETURNING transfer_id, blob_id, blob_size, path;
            ",
            [
                Box::new(local_repo_id) as Box<dyn ToSql + Send + Sync>,
                Box::new(remote_repo_id),
                Box::new(serde_json::to_string(&paths).unwrap()),
                Box::new(serde_json::to_string(&paths).unwrap()),
                Box::new(transfer_id),
            ],
            |row| {
                Ok(FileTransferItem {
                    transfer_id: row.get::<_, i64>(0)? as u32,
                    blob_id: row.get::<_, String>(1)?,
                    blob_size: row.get::<_, i64>(2)?,
                    path: row.get::<_, String>(3)?,
                })
            },
        )
    }
    pub(crate) async fn select_files_transfer(
        &self,
        input_stream: impl Stream<Item = CopiedTransferItem> + Unpin + Send + 'static,
    ) -> DBOutputStream<'static, FileTransferItem> {
        let s = self.clone();

        input_stream
            .bounded_wait_chunks(self.temp_table_max_rows, time::Duration::from_millis(1))
            .then(move |chunk: Vec<CopiedTransferItem>| {
                let s = s.clone();
                Box::pin(async move {
                    if chunk.is_empty() {
                        return stream::iter(vec![]).boxed();
                    }

                    let rows: Result<Vec<FileTransferItem>, DBError> = s
                        .pool
                        .with_conn(move |conn| {
                            conn.execute_batch(
                                "CREATE TEMP TABLE tmp_transfers_input(
                                transfer_id BIGINT,
                                path TEXT
                            );",
                            )?;

                            {
                                let mut app = conn.appender("tmp_transfers_input")?;
                                for r in &chunk {
                                    let params: [Box<dyn ToSql + Send + Sync>; 2] =
                                        [Box::new(r.transfer_id as i64), Box::new(r.path.clone())];
                                    app.append_row(params)?;
                                }
                            }

                            let mut stmt = conn.prepare(
                                "
                            SELECT transfer_id, blob_id, blob_size, path
                            FROM transfers
                            INNER JOIN tmp_transfers_input USING (transfer_id, path);
                            ",
                            )?;
                            let rows = stmt
                                .query_map([], |row| {
                                    Ok(FileTransferItem {
                                        transfer_id: row.get::<_, i64>(0)? as u32,
                                        blob_id: row.get::<_, String>(1)?,
                                        blob_size: row.get::<_, i64>(2)?,
                                        path: row.get::<_, String>(3)?,
                                    })
                                })?
                                .collect::<Result<Vec<_>, _>>()?;

                            let _ = conn.execute_batch("DROP TABLE tmp_transfers_input;");
                            Ok(rows)
                        })
                        .await;

                    stream::iter(match rows {
                        Ok(v) => v.into_iter().map(Ok).collect::<Vec<_>>(),
                        Err(e) => vec![Err(e)],
                    })
                    .boxed()
                })
            })
            .flatten()
            .boxed()
            .track("Database::select_files_transfer")
            .boxed()
    }
}
