#[cfg(test)]
mod tests {
    use crate::db::database::Database;
    use crate::db::migrations::run_migrations;

    use crate::db::models::{
        Blob, File, InsertBlob, InsertFile, InsertMaterialisation, InsertRepositoryName, Repository,
    };
    use crate::utils::flow::{ExtFlow, Flow};
    use chrono::Utc;
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

    #[tokio::test]
    async fn test_get_or_create_current_repository() {
        let db = setup_test_db().await;
        let repo1 = db
            .get_or_create_current_repository()
            .await
            .expect("repo creation failed");
        let repo2 = db
            .get_or_create_current_repository()
            .await
            .expect("repo creation failed");
        assert_eq!(
            repo1.repo_id, repo2.repo_id,
            "Repository IDs should be equal on repeated calls"
        );
    }

    #[tokio::test]
    async fn test_lookup_current_repository_name() {
        let db = setup_test_db().await;
        let repo = db
            .get_or_create_current_repository()
            .await
            .expect("failed to get repo");

        // Initially, there is no name.
        let name = db
            .lookup_current_repository_name(repo.repo_id.clone())
            .await
            .expect("lookup failed");
        assert!(name.is_none(), "Expected no name initially");

        // Add a repository name.
        let insert_name = InsertRepositoryName {
            repo_id: repo.repo_id.clone(),
            name: "Test Repository".into(),
            valid_from: Utc::now(),
        };
        let count = db
            .add_repository_names(stream::iter([insert_name]))
            .await
            .expect("failed to add repository name");
        assert_eq!(count, 1, "Expected one repository name added");

        let name = db
            .lookup_current_repository_name(repo.repo_id.clone())
            .await
            .expect("lookup failed");
        assert_eq!(
            name,
            Some("Test Repository".to_string()),
            "Repository name should be updated"
        );
    }

    #[tokio::test]
    async fn test_add_and_select_files() {
        let db = setup_test_db().await;
        let now = Utc::now();

        let file1 = InsertFile {
            path: "file1.txt".into(),
            blob_id: Some("blob1".into()),
            valid_from: now,
        };
        let file2 = InsertFile {
            path: "file2.txt".into(),
            blob_id: Some("blob2".into()),
            valid_from: now,
        };
        let count = db
            .add_files(stream::iter([file1, file2]))
            .await
            .expect("failed to add files");
        assert_eq!(count, 2, "Two files should be added");

        let mut stream = db.select_files(-1).await;
        let mut paths = Vec::new();
        while let Some(result) = stream.next().await {
            let file: File = result.expect("select_files failed");
            paths.push(file.path);
        }
        assert!(
            paths.contains(&"file1.txt".to_string()),
            "Expected file1.txt in the query results"
        );
        assert!(
            paths.contains(&"file2.txt".to_string()),
            "Expected file2.txt in the query results"
        );
    }

    #[tokio::test]
    async fn test_add_and_select_blobs() {
        let db = setup_test_db().await;
        let now = Utc::now();
        let repo = db
            .get_or_create_current_repository()
            .await
            .expect("failed to get repo");

        let blob1 = InsertBlob {
            repo_id: repo.repo_id.clone(),
            blob_id: "blob1".into(),
            blob_size: 100,
            has_blob: true,
            path: Some("path1".into()),
            valid_from: now,
        };
        let blob2 = InsertBlob {
            repo_id: repo.repo_id.clone(),
            blob_id: "blob2".into(),
            blob_size: 200,
            has_blob: false,
            path: Some("path2".into()),
            valid_from: now,
        };
        let count = db
            .add_blobs(stream::iter([blob1, blob2]))
            .await
            .expect("failed to add blobs");
        assert_eq!(count, 2, "Two blobs should be added");

        let mut stream = db.select_blobs(-1).await;
        let mut blob_ids = Vec::new();
        while let Some(result) = stream.next().await {
            let blob: Blob = result.expect("select_blobs failed");
            blob_ids.push(blob.blob_id);
        }
        assert!(blob_ids.contains(&"blob1".to_string()));
        assert!(blob_ids.contains(&"blob2".to_string()));
    }

    #[tokio::test]
    async fn test_merge_repositories() {
        let db = setup_test_db().await;
        let repo_id = "test_repo".to_string();

        // Insert initial repository record.
        let repo_initial = Repository {
            repo_id: repo_id.clone(),
            last_file_index: 0,
            last_blob_index: 0,
            last_name_index: 0,
        };
        db.merge_repositories(stream::iter([repo_initial]))
            .await
            .expect("initial merge failed");

        // Merge updated values.
        let repo_update = Repository {
            repo_id: repo_id.clone(),
            last_file_index: 10,
            last_blob_index: 20,
            last_name_index: 5,
        };
        db.merge_repositories(stream::iter([repo_update]))
            .await
            .expect("merge update failed");

        let repo = db
            .lookup_repository(repo_id.clone())
            .await
            .expect("lookup_repository failed");
        assert_eq!(repo.last_file_index, 10);
        assert_eq!(repo.last_blob_index, 20);
        assert_eq!(repo.last_name_index, 5);
    }

    #[tokio::test]
    async fn test_update_last_indices() {
        let db = setup_test_db().await;
        let repo = db.get_or_create_current_repository().await.expect("failed");

        let now = Utc::now();
        let file = InsertFile {
            path: "update_test.txt".into(),
            blob_id: Some("blob_up".into()),
            valid_from: now,
        };
        let blob = InsertBlob {
            repo_id: repo.repo_id.clone(),
            blob_id: "blob_up".into(),
            blob_size: 123,
            has_blob: true,
            path: Some("update_test.txt".into()),
            valid_from: now,
        };
        let name = InsertRepositoryName {
            repo_id: repo.repo_id.clone(),
            name: "test".into(),
            valid_from: now,
        };
        db.add_files(stream::iter([file])).await.expect("failed");
        db.add_blobs(stream::iter([blob])).await.expect("failed");
        db.add_repository_names(stream::iter([name]))
            .await
            .expect("failed");

        let updated_repo = db
            .update_last_indices()
            .await
            .expect("update_last_indices failed");
        assert!(updated_repo.last_file_index > 0);
        assert!(updated_repo.last_blob_index > 0);
        assert!(updated_repo.last_name_index > 0);
    }

    #[tokio::test]
    async fn test_available_and_missing_blobs() {
        let db = setup_test_db().await;
        let repo = db.get_or_create_current_repository().await.expect("failed");
        let now = Utc::now();

        let avail_blob = InsertBlob {
            repo_id: repo.repo_id.clone(),
            blob_id: "avail_blob".into(),
            blob_size: 50,
            has_blob: true,
            path: Some("avail.txt".into()),
            valid_from: now,
        };
        db.add_blobs(stream::iter([avail_blob]))
            .await
            .expect("failed");

        let missing_file = InsertFile {
            path: "missing.txt".into(),
            blob_id: Some("missing_blob".into()),
            valid_from: now,
        };
        db.add_files(stream::iter([missing_file]))
            .await
            .expect("failed");

        let mut avail_stream = db.available_blobs(repo.repo_id.clone());
        let mut available = Vec::new();
        while let Some(result) = avail_stream.next().await {
            available.push(result.expect("available_blobs query failed"));
        }
        assert!(
            available.iter().any(|b| b.blob_id == "avail_blob"),
            "Expected available blob 'avail_blob'"
        );

        let mut missing_stream = db.missing_blobs(repo.repo_id.clone());
        let mut missing = Vec::new();
        while let Some(result) = missing_stream.next().await {
            missing.push(result.expect("missing_blobs query failed"));
        }
        assert!(
            missing.iter().any(|b| b.blob_id == "missing_blob"),
            "Expected missing blob 'missing_blob'"
        );
    }

    #[tokio::test]
    async fn test_lookup_last_materialisation() {
        let db = setup_test_db().await;
        let now = Utc::now();

        let mat = InsertMaterialisation {
            path: "mat_file.txt".into(),
            blob_id: Some("mat_blob".into()),
            valid_from: now,
        };
        db.add_materialisations(stream::iter([mat]))
            .await
            .expect("failed to add materialisation");

        let lookup = db
            .lookup_last_materialisation("mat_file.txt".into())
            .await
            .expect("lookup failed");
        assert_eq!(
            lookup.unwrap().blob_id,
            Some("mat_blob".into()),
            "Materialisation should match"
        );
    }

    #[tokio::test]
    async fn test_virtual_filesystem_operations() {
        let db = setup_test_db().await;
        // Truncate the virtual filesystem.
        db.truncate_virtual_filesystem()
            .await
            .expect("truncate_virtual_filesystem failed");

        // Create an observation (using the FileSeen variant).
        use crate::db::models::{FileSeen, Observation};
        let now = Utc::now();
        let obs = Observation::FileSeen(FileSeen {
            path: "vfs_file.txt".into(),
            seen_id: 200,
            seen_dttm: now.timestamp(),
            last_modified_dttm: now.timestamp(),
            size: 512,
        });
        let observations = [Flow::Data(obs)];
        let mut obs_stream = db
            .add_virtual_filesystem_observations(futures::stream::iter(observations))
            .await;
        while let Some(item) = obs_stream.next().await {
            match item {
                ExtFlow::Data(data) => {
                    data.expect("processing virtual filesystem observation failed");
                }
                ExtFlow::Shutdown(_) => panic!("unexpected processing shutdown"),
            }
        }
        // Select missing files (if any).
        let mut missing_stream = db.select_missing_files_on_virtual_filesystem(0).await;
        // Simply consume the stream.
        while let Some(_mf) = missing_stream.next().await {
            // For now, just ensure that the query runs.
        }
    }

    #[tokio::test]
    async fn test_connection_methods() {
        let db = setup_test_db().await;
        use crate::db::models::Connection;

        // Initially, list should be empty.
        let list = db.list_all_connections().await.expect("list failed");
        assert!(list.is_empty(), "Expected no connections initially");

        let conn = Connection {
            name: "conn1".into(),
            connection_type: crate::db::models::ConnectionType::Local,
            parameter: "/tmp".into(),
        };
        db.add_connection(&conn)
            .await
            .expect("failed to add connection");

        let lookup = db
            .connection_by_name("conn1")
            .await
            .expect("connection lookup failed");
        assert!(lookup.is_some(), "Connection 'conn1' should be found");
        assert_eq!(lookup.unwrap().parameter, "/tmp");

        let list = db.list_all_connections().await.expect("list failed");
        assert_eq!(list.len(), 1, "Expected one connection in list");
    }

    #[tokio::test]
    async fn test_populate_and_select_missing_blobs_transfer() {
        let db = setup_test_db().await;
        let now = Utc::now();

        let remote_blob = InsertBlob {
            repo_id: "remote_repo".into(),
            blob_id: "remote_blob".into(),
            blob_size: 50,
            has_blob: true,
            path: None,
            valid_from: now,
        };
        db.add_blobs(stream::iter([remote_blob]))
            .await
            .expect("failed to add remote blob");

        let file = InsertFile {
            path: "ath.txt".into(),
            blob_id: Some("remote_blob".into()),
            valid_from: now,
        };
        db.add_files(stream::iter([file]))
            .await
            .expect("failed to add file");

        let transfer_id = 123;
        let mut populate_stream = db
            .populate_missing_blobs_for_transfer(transfer_id, "remote_repo".into())
            .await;
        let mut populated = Vec::new();
        while let Some(item) = populate_stream.next().await {
            populated.push(item.expect("failed to populate missing blobs"));
        }
        assert!(
            !populated.is_empty(),
            "Expected at least one missing blob transfer item"
        );

        let mut select_stream = db.select_blobs_transfer(transfer_id).await;
        let mut selected = Vec::new();
        while let Some(item) = select_stream.next().await {
            selected.push(item.expect("failed to select blobs transfer"));
        }
        assert_eq!(
            selected.len(),
            populated.len(),
            "The number of selected transfer items should match the populated count"
        );
    }

    #[tokio::test]
    async fn test_populate_and_select_missing_files_transfer() {
        let db = setup_test_db().await;
        let now = Utc::now();
        let repo = db.get_or_create_current_repository().await.expect("failed");

        let file = InsertFile {
            path: "missing_file.txt".into(),
            blob_id: Some("remote_blob".into()),
            valid_from: now,
        };
        db.add_files(stream::iter([file])).await.expect("failed");

        let remote_blob = InsertBlob {
            repo_id: "remote_repo".into(),
            blob_id: "remote_blob".into(),
            blob_size: 75,
            has_blob: true,
            path: Some("missing_file.txt".into()),
            valid_from: now,
        };
        db.add_blobs(stream::iter([remote_blob]))
            .await
            .expect("failed");

        let transfer_id = 456;
        let mut populate_stream = db
            .populate_missing_files_for_transfer(
                transfer_id,
                repo.repo_id.clone(),
                "remote_repo".into(),
            )
            .await;
        let mut populated = Vec::new();
        while let Some(item) = populate_stream.next().await {
            populated.push(item.expect("failed to populate missing files"));
        }
        assert!(
            !populated.is_empty(),
            "Expected at least one missing file transfer item"
        );

        let mut select_stream = db.select_files_transfer(transfer_id).await;
        let mut selected = Vec::new();
        while let Some(item) = select_stream.next().await {
            selected.push(item.expect("failed to select files transfer"));
        }
        assert_eq!(
            selected.len(),
            populated.len(),
            "Mismatch in missing file transfer items count"
        );
    }
}
