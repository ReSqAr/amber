#[cfg(test)]
use chrono::TimeZone;

use crate::db::database::Database;
use crate::db::migrations::run_migrations;
use crate::db::models;
use crate::db::models::{
    FileEqBlobCheck, FileSeen, InsertBlob, InsertFile, Observation, VirtualFileState,
};
use crate::utils::flow::{ExtFlow, Flow};
use chrono::{DateTime, Utc};
use futures::stream;
use futures::StreamExt;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};

async fn setup_test_db() -> Database {
    let pool: SqlitePool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(":memory:")
        .await
        .expect("failed to create pool");

    run_migrations(&pool)
        .await
        .expect("failed to run migrations");

    Database::new(pool)
}

const BEGINNING: i64 = 1577836800; // 2020-01-01T00:00:00Z

pub(crate) struct TestFile {
    pub path: String,
    pub blob_id: Option<String>,
    pub valid_from: DateTime<Utc>,
}

async fn seed_files(db: &Database, test_files: impl IntoIterator<Item = TestFile>) {
    let inserts: Vec<InsertFile> = test_files
        .into_iter()
        .map(|tf| InsertFile {
            path: tf.path,
            blob_id: tf.blob_id,
            valid_from: tf.valid_from,
        })
        .collect();
    let file_stream = stream::iter(inserts);
    db.add_files(file_stream)
        .await
        .expect("failed to add files");
}

pub(crate) struct TestBlob {
    pub blob_id: String,
    pub blob_size: i64,
    pub has_blob: bool,
    pub valid_from: DateTime<Utc>,
}

async fn seed_blobs(db: &Database, test_blobs: impl IntoIterator<Item = TestBlob>) {
    let repo_id = db
        .get_or_create_current_repository()
        .await
        .expect("failed to get repository")
        .repo_id;
    let inserts: Vec<InsertBlob> = test_blobs
        .into_iter()
        .map(|tb| InsertBlob {
            repo_id: repo_id.clone(),
            blob_id: tb.blob_id,
            blob_size: tb.blob_size,
            has_blob: tb.has_blob,
            valid_from: tb.valid_from,
        })
        .collect();

    db.add_blobs(stream::iter(inserts))
        .await
        .expect("failed to add blobs");
}

async fn refresh_vfs(db: &Database) {
    db.refresh_virtual_filesystem()
        .await
        .expect("failed to refresh virtual filesystem");
}

pub(crate) async fn apply_observations(
    db: &Database,
    observations: impl IntoIterator<Item = models::Observation>,
) -> std::collections::HashMap<String, VirtualFileState> {
    let observations: Vec<_> = observations.into_iter().map(Flow::Data).collect();
    let input_stream = stream::iter(observations);
    let mut output_stream = db.add_virtual_filesystem_observations(input_stream).await;
    let mut mapping = std::collections::HashMap::new();
    while let Some(item) = output_stream.next().await {
        match item {
            ExtFlow::Data(Ok(vfs)) | ExtFlow::Shutdown(Ok(vfs)) => {
                for vf in vfs {
                    mapping.insert(vf.path.clone(), vf.state.unwrap());
                }
            }
            ExtFlow::Data(Err(e)) | ExtFlow::Shutdown(Err(e)) => {
                panic!("Error applying observations: {}", e);
            }
        }
    }
    mapping
}

pub(crate) async fn setup(
    blobs: impl IntoIterator<Item = TestBlob>,
    files: impl IntoIterator<Item = TestFile>,
    seen_files: impl IntoIterator<Item = FileSeen>,
    eq_blob_check: impl IntoIterator<Item = FileEqBlobCheck>,
) -> Database {
    let db = setup_test_db().await;

    seed_files(&db, files).await;
    seed_blobs(&db, blobs).await;
    refresh_vfs(&db).await;

    let seen_files: Vec<_> = seen_files.into_iter().map(Observation::FileSeen).collect();
    let eq_blob_check: Vec<_> = eq_blob_check
        .into_iter()
        .map(Observation::FileEqBlobCheck)
        .collect();
    let mut observations = seen_files;
    observations.extend(eq_blob_check);
    apply_observations(&db, observations).await;

    db
}

#[tokio::test]
async fn test_new_file() {
    let files = [];
    let blobs = [];
    let seen_files = [];
    let eq_blob_check = [];
    let db = setup(blobs, files, seen_files, eq_blob_check).await;

    let new_obs = [Observation::FileSeen(FileSeen {
        path: "test".into(),
        last_seen_id: 42 + 1,
        last_seen_dttm: BEGINNING + 30,
        last_modified_dttm: BEGINNING + 20,
        size: 84,
    })];

    let state_map = apply_observations(&db, new_obs).await;

    assert_eq!(
        state_map.get("test"),
        Some(&VirtualFileState::New),
        "Expected file 'test' to be in New state"
    );
}

#[tokio::test]
async fn test_ok_state_for_repeated_observation() {
    let files = [TestFile {
        path: "test".into(),
        blob_id: Some("blob1".into()),
        valid_from: Utc.timestamp_opt(BEGINNING + 20, 0).unwrap(),
    }];
    let blobs = [TestBlob {
        blob_id: "blob1".into(),
        blob_size: 84,
        has_blob: true,
        valid_from: Utc.timestamp_opt(BEGINNING + 20, 0).unwrap(),
    }];
    let seen_files = [FileSeen {
        path: "test".into(),
        last_seen_id: 42,
        last_seen_dttm: BEGINNING + 30,
        last_modified_dttm: BEGINNING + 20,
        size: 84,
    }];
    let eq_blob_check = [FileEqBlobCheck {
        path: "test".into(),
        last_check_dttm: BEGINNING + 30,
        last_result: true,
    }];
    let db = setup(blobs, files, seen_files, eq_blob_check).await;

    let new_obs = [Observation::FileSeen(FileSeen {
        path: "test".into(),
        last_seen_id: 42 + 1,
        last_seen_dttm: BEGINNING + 30,
        last_modified_dttm: BEGINNING + 20,
        size: 84,
    })];

    let state_map = apply_observations(&db, new_obs).await;

    assert_eq!(
        state_map.get("test"),
        Some(&VirtualFileState::Ok),
        "Expected file 'test' to be in Ok state"
    );
}

#[tokio::test]
async fn test_modified_size() {
    let files = [TestFile {
        path: "test".into(),
        blob_id: Some("blob1".into()),
        valid_from: Utc.timestamp_opt(BEGINNING + 20, 0).unwrap(),
    }];
    let blobs = [TestBlob {
        blob_id: "blob1".into(),
        blob_size: 84,
        has_blob: true,
        valid_from: Utc.timestamp_opt(BEGINNING + 20, 0).unwrap(),
    }];
    let seen_files = [FileSeen {
        path: "test".into(),
        last_seen_id: 42,
        last_seen_dttm: BEGINNING + 30,
        last_modified_dttm: BEGINNING + 20,
        size: 84,
    }];
    let eq_blob_check = [FileEqBlobCheck {
        path: "test".into(),
        last_check_dttm: BEGINNING + 30,
        last_result: true,
    }];
    let db = setup(blobs, files, seen_files, eq_blob_check).await;

    let new_obs = [Observation::FileSeen(FileSeen {
        path: "test".into(),
        last_seen_id: 42 + 1,
        last_seen_dttm: BEGINNING + 30,
        last_modified_dttm: BEGINNING + 20,
        size: 4242,
    })];

    let state_map = apply_observations(&db, new_obs).await;

    assert_eq!(
        state_map.get("test"),
        Some(&VirtualFileState::NeedsCheck),
        "Expected file 'test' to be in NeedsCheck state"
    );
}

#[tokio::test]
async fn test_modified_last_modified() {
    let files = [TestFile {
        path: "test".into(),
        blob_id: Some("blob1".into()),
        valid_from: Utc.timestamp_opt(BEGINNING + 20, 0).unwrap(),
    }];
    let blobs = [TestBlob {
        blob_id: "blob1".into(),
        blob_size: 84,
        has_blob: true,
        valid_from: Utc.timestamp_opt(BEGINNING + 20, 0).unwrap(),
    }];
    let seen_files = [FileSeen {
        path: "test".into(),
        last_seen_id: 42,
        last_seen_dttm: BEGINNING + 30,
        last_modified_dttm: BEGINNING + 20,
        size: 84,
    }];
    let eq_blob_check = [FileEqBlobCheck {
        path: "test".into(),
        last_check_dttm: BEGINNING + 30,
        last_result: true,
    }];
    let db = setup(blobs, files, seen_files, eq_blob_check).await;

    let new_obs = [Observation::FileSeen(FileSeen {
        path: "test".into(),
        last_seen_id: 42 + 1,
        last_seen_dttm: BEGINNING + 30,
        last_modified_dttm: BEGINNING + 40,
        size: 84,
    })];

    let state_map = apply_observations(&db, new_obs).await;

    assert_eq!(
        state_map.get("test"),
        Some(&VirtualFileState::NeedsCheck),
        "Expected file 'test' to be in NeedsCheck state"
    );
}

#[tokio::test]
async fn test_check_true() {
    let files = [TestFile {
        path: "test".into(),
        blob_id: Some("blob1".into()),
        valid_from: Utc.timestamp_opt(BEGINNING + 20, 0).unwrap(),
    }];
    let blobs = [TestBlob {
        blob_id: "blob1".into(),
        blob_size: 84,
        has_blob: true,
        valid_from: Utc.timestamp_opt(BEGINNING + 20, 0).unwrap(),
    }];
    let seen_files = [FileSeen {
        path: "test".into(),
        last_seen_id: 42,
        last_seen_dttm: BEGINNING + 30,
        last_modified_dttm: BEGINNING + 20,
        size: 84,
    }];
    let eq_blob_check = [];
    let db = setup(blobs, files, seen_files, eq_blob_check).await;

    let new_obs = [Observation::FileEqBlobCheck(FileEqBlobCheck {
        path: "test".into(),
        last_check_dttm: BEGINNING + 30,
        last_result: true,
    })];

    let state_map = apply_observations(&db, new_obs).await;

    assert_eq!(
        state_map.get("test"),
        Some(&VirtualFileState::Ok),
        "Expected file 'test' to be in Ok state"
    );
}

#[tokio::test]
async fn test_check_false() {
    let files = [TestFile {
        path: "test".into(),
        blob_id: Some("blob1".into()),
        valid_from: Utc.timestamp_opt(BEGINNING + 20, 0).unwrap(),
    }];
    let blobs = [TestBlob {
        blob_id: "blob1".into(),
        blob_size: 84,
        has_blob: true,
        valid_from: Utc.timestamp_opt(BEGINNING + 20, 0).unwrap(),
    }];
    let seen_files = [FileSeen {
        path: "test".into(),
        last_seen_id: 42,
        last_seen_dttm: BEGINNING + 30,
        last_modified_dttm: BEGINNING + 20,
        size: 84,
    }];
    let eq_blob_check = [];
    let db = setup(blobs, files, seen_files, eq_blob_check).await;

    let new_obs = [Observation::FileEqBlobCheck(FileEqBlobCheck {
        path: "test".into(),
        last_check_dttm: BEGINNING + 30,
        last_result: false,
    })];

    let state_map = apply_observations(&db, new_obs).await;

    assert_eq!(
        state_map.get("test"),
        Some(&VirtualFileState::Dirty),
        "Expected file 'test' to be in Dirty state"
    );
}
