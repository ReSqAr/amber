#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use crate::db;
    use crate::db::database::Database;
    use crate::db::models::{
        FileCheck, FileSeen, InsertBlob, InsertFile, InsertMaterialisation, Observation,
        VirtualFileState,
    };
    use crate::utils::flow::{ExtFlow, Flow};
    use chrono::{DateTime, Utc};
    use futures::StreamExt;
    use futures::stream;
    use tempfile::tempdir;

    const BEGINNING: i64 = 1577836800; // 2020-01-01T00:00:00Z

    async fn setup_test_db() -> (Database, tempfile::TempDir) {
        let t = tempdir().unwrap();
        (db::open(t.path()).await.unwrap(), t)
    }

    struct TestBlob {
        blob_id: String,
        blob_size: i64,
        has_blob: bool,
        valid_from: DateTime<Utc>,
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
                path: None,
                valid_from: tb.valid_from,
            })
            .collect();

        db.add_blobs(stream::iter(inserts))
            .await
            .expect("failed to add blobs");
    }

    struct TestFile {
        path: String,
        blob_id: Option<String>,
        valid_from: DateTime<Utc>,
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
        db.add_files(stream::iter(inserts))
            .await
            .expect("failed to add files");
    }

    struct TestMaterialisation {
        path: String,
        blob_id: Option<String>,
        valid_from: DateTime<Utc>,
    }

    async fn seed_materialisations(
        db: &Database,
        test_materialisations: impl IntoIterator<Item = TestMaterialisation>,
    ) {
        let inserts: Vec<InsertMaterialisation> = test_materialisations
            .into_iter()
            .map(|tf| InsertMaterialisation {
                path: tf.path,
                blob_id: tf.blob_id,
                valid_from: tf.valid_from,
            })
            .collect();
        db.add_materialisations(stream::iter(inserts))
            .await
            .expect("failed to add files");
    }

    async fn refresh_vfs(db: &Database) {
        db.refresh_virtual_filesystem()
            .await
            .expect("failed to refresh virtual filesystem");
    }

    async fn apply_observations(
        db: &Database,
        observations: impl IntoIterator<Item = Observation>,
    ) -> std::collections::HashMap<String, VirtualFileState> {
        let observations: Vec<_> = observations.into_iter().map(Flow::Data).collect();
        let input_stream = stream::iter(observations);
        let mut output_stream = db.add_virtual_filesystem_observations(input_stream).await;
        let mut mapping = std::collections::HashMap::new();
        while let Some(item) = output_stream.next().await {
            match item {
                ExtFlow::Data(Ok(vfs)) | ExtFlow::Shutdown(Ok(vfs)) => {
                    for vf in vfs {
                        mapping.insert(vf.path.clone(), vf.state);
                    }
                }
                ExtFlow::Data(Err(e)) | ExtFlow::Shutdown(Err(e)) => {
                    panic!("Error applying observations: {}", e);
                }
            }
        }
        mapping
    }

    async fn setup(
        blobs: impl IntoIterator<Item = TestBlob>,
        files: impl IntoIterator<Item = TestFile>,
        materialisations: impl IntoIterator<Item = TestMaterialisation>,
        seen_files: impl IntoIterator<Item = FileSeen>,
        eq_blob_check: impl IntoIterator<Item = FileCheck>,
    ) -> Database {
        let (db, _t) = setup_test_db().await;

        seed_files(&db, files).await;
        seed_blobs(&db, blobs).await;
        seed_materialisations(&db, materialisations).await;
        refresh_vfs(&db).await;

        let seen_files: Vec<_> = seen_files.into_iter().map(Observation::FileSeen).collect();
        let eq_blob_check: Vec<_> = eq_blob_check
            .into_iter()
            .map(Observation::FileCheck)
            .collect();
        let mut observations = seen_files;
        observations.extend(eq_blob_check);
        apply_observations(&db, observations).await;

        db
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_new_file() {
        let files = [];
        let blobs = [];
        let mtrlstns = [];
        let seen_files = [];
        let eq_blob_check = [];
        let db = setup(blobs, files, mtrlstns, seen_files, eq_blob_check).await;

        let new_obs = [Observation::FileSeen(FileSeen {
            path: "test".into(),
            seen_id: 42 + 1,
            seen_dttm: BEGINNING + 30,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        let mtrlstns = [TestMaterialisation {
            path: "test".to_string(),
            blob_id: Some("blob1".into()),
            valid_from: Utc.timestamp_opt(BEGINNING + 20, 0).unwrap(),
        }];
        let seen_files = [FileSeen {
            path: "test".into(),
            seen_id: 42,
            seen_dttm: BEGINNING + 30,
            last_modified_dttm: BEGINNING + 20,
            size: 84,
        }];
        let eq_blob_check = [FileCheck {
            path: "test".into(),
            check_dttm: BEGINNING + 30,
            hash: "blob1".into(),
        }];
        let db = setup(blobs, files, mtrlstns, seen_files, eq_blob_check).await;

        let new_obs = [Observation::FileSeen(FileSeen {
            path: "test".into(),
            seen_id: 42 + 1,
            seen_dttm: BEGINNING + 30,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        let mtrlstns = [];
        let seen_files = [FileSeen {
            path: "test".into(),
            seen_id: 42,
            seen_dttm: BEGINNING + 30,
            last_modified_dttm: BEGINNING + 20,
            size: 84,
        }];
        let eq_blob_check = [FileCheck {
            path: "test".into(),
            check_dttm: BEGINNING + 30,
            hash: "blob1".into(),
        }];
        let db = setup(blobs, files, mtrlstns, seen_files, eq_blob_check).await;

        let new_obs = [Observation::FileSeen(FileSeen {
            path: "test".into(),
            seen_id: 42 + 1,
            seen_dttm: BEGINNING + 30,
            last_modified_dttm: BEGINNING + 20,
            size: 4242,
        })];

        let state_map = apply_observations(&db, new_obs).await;

        assert_eq!(
            state_map.get("test"),
            Some(&VirtualFileState::CorruptionDetected),
            "Expected file 'test' to be in CorruptionDetected state"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        let mtrlstns = [];
        let seen_files = [FileSeen {
            path: "test".into(),
            seen_id: 42,
            seen_dttm: BEGINNING + 30,
            last_modified_dttm: BEGINNING + 20,
            size: 84,
        }];
        let eq_blob_check = [FileCheck {
            path: "test".into(),
            check_dttm: BEGINNING + 30,
            hash: "blob1".into(),
        }];
        let db = setup(blobs, files, mtrlstns, seen_files, eq_blob_check).await;

        let new_obs = [Observation::FileSeen(FileSeen {
            path: "test".into(),
            seen_id: 42 + 1,
            seen_dttm: BEGINNING + 30,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_check_same_file() {
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
        let mtrlstns = [TestMaterialisation {
            path: "test".to_string(),
            blob_id: Some("blob1".into()),
            valid_from: Utc.timestamp_opt(BEGINNING + 20, 0).unwrap(),
        }];
        let seen_files = [FileSeen {
            path: "test".into(),
            seen_id: 42,
            seen_dttm: BEGINNING + 30,
            last_modified_dttm: BEGINNING + 20,
            size: 84,
        }];
        let eq_blob_check = [];
        let db = setup(blobs, files, mtrlstns, seen_files, eq_blob_check).await;

        let new_obs = [Observation::FileCheck(FileCheck {
            path: "test".into(),
            check_dttm: BEGINNING + 30,
            hash: "blob1".into(),
        })];

        let state_map = apply_observations(&db, new_obs).await;

        assert_eq!(
            state_map.get("test"),
            Some(&VirtualFileState::Ok),
            "Expected file 'test' to be in Ok state"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_altered() {
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
        let mtrlstns = [];
        let seen_files = [FileSeen {
            path: "test".into(),
            seen_id: 42,
            seen_dttm: BEGINNING + 30,
            last_modified_dttm: BEGINNING + 20,
            size: 84,
        }];
        let eq_blob_check = [];
        let db = setup(blobs, files, mtrlstns, seen_files, eq_blob_check).await;

        let new_obs = [Observation::FileCheck(FileCheck {
            path: "test".into(),
            check_dttm: BEGINNING + 30,
            hash: "other".into(),
        })];

        let state_map = apply_observations(&db, new_obs).await;

        assert_eq!(
            state_map.get("test"),
            Some(&VirtualFileState::Altered),
            "Expected file 'test' to be in Altered state"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_outdated() {
        let files = [
            TestFile {
                path: "test".into(),
                blob_id: Some("old".into()),
                valid_from: Utc.timestamp_opt(BEGINNING + 10, 0).unwrap(),
            },
            TestFile {
                path: "test".into(),
                blob_id: Some("new".into()),
                valid_from: Utc.timestamp_opt(BEGINNING + 30, 0).unwrap(),
            },
        ];
        let blobs = [
            TestBlob {
                blob_id: "old".into(),
                blob_size: 42,
                has_blob: true,
                valid_from: Utc.timestamp_opt(BEGINNING + 10, 0).unwrap(),
            },
            TestBlob {
                blob_id: "new".into(),
                blob_size: 84,
                has_blob: true,
                valid_from: Utc.timestamp_opt(BEGINNING + 30, 0).unwrap(),
            },
        ];
        let mtrlstns = [TestMaterialisation {
            path: "test".into(),
            blob_id: Some("old".to_string()),
            valid_from: Utc.timestamp_opt(BEGINNING + 10, 0).unwrap(),
        }];
        let seen_files = [FileSeen {
            path: "test".into(),
            seen_id: 42,
            seen_dttm: BEGINNING + 20,
            last_modified_dttm: BEGINNING + 20,
            size: 42,
        }];
        let eq_blob_check = [FileCheck {
            path: "test".into(),
            check_dttm: BEGINNING + 20,
            hash: "old".into(),
        }];
        let db = setup(blobs, files, mtrlstns, seen_files, eq_blob_check).await;

        let new_obs = [Observation::FileSeen(FileSeen {
            path: "test".into(),
            seen_id: 42,
            seen_dttm: BEGINNING + 40,
            last_modified_dttm: BEGINNING + 20,
            size: 42,
        })];

        let state_map = apply_observations(&db, new_obs).await;

        assert_eq!(
            state_map.get("test"),
            Some(&VirtualFileState::Outdated),
            "Expected file 'test' to be in Outdated state"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_deleted() {
        let files = [
            TestFile {
                path: "test".into(),
                blob_id: Some("old".into()),
                valid_from: Utc.timestamp_opt(BEGINNING + 10, 0).unwrap(),
            },
            TestFile {
                path: "test".into(),
                blob_id: None,
                valid_from: Utc.timestamp_opt(BEGINNING + 30, 0).unwrap(),
            },
        ];
        let blobs = [TestBlob {
            blob_id: "old".into(),
            blob_size: 42,
            has_blob: true,
            valid_from: Utc.timestamp_opt(BEGINNING + 10, 0).unwrap(),
        }];
        let mtrlstns = [TestMaterialisation {
            path: "test".into(),
            blob_id: Some("old".to_string()),
            valid_from: Utc.timestamp_opt(BEGINNING + 10, 0).unwrap(),
        }];
        let seen_files = [FileSeen {
            path: "test".into(),
            seen_id: 42,
            seen_dttm: BEGINNING + 20,
            last_modified_dttm: BEGINNING + 20,
            size: 42,
        }];
        let eq_blob_check = [FileCheck {
            path: "test".into(),
            check_dttm: BEGINNING + 20,
            hash: "old".into(),
        }];
        let db = setup(blobs, files, mtrlstns, seen_files, eq_blob_check).await;

        let new_obs = [Observation::FileSeen(FileSeen {
            path: "test".into(),
            seen_id: 42,
            seen_dttm: BEGINNING + 40,
            last_modified_dttm: BEGINNING + 20,
            size: 42,
        })];

        let state_map = apply_observations(&db, new_obs).await;

        assert_eq!(
            state_map.get("test"),
            Some(&VirtualFileState::Outdated),
            "Expected file 'test' to be in Outdated state"
        );
    }
}
