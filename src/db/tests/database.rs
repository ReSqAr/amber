#[cfg(test)]
mod tests {
    use crate::db::models::{FileEqBlobCheck, FileSeen, Observation, VirtualFileState};
    use crate::db::tests::helper::{apply_observations, setup, TestBlob, TestFile};
    use chrono::{TimeZone, Utc};

    const BEGINNING: i64 = 1577836800; // 2020-01-01T00:00:00Z

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
}
