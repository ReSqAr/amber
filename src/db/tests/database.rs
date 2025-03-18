#[cfg(test)]
mod tests {
    use crate::db::database::Database;
    use crate::db::migrations::run_migrations;

    use crate::db::models::{
        AvailableBlob, Blob, Connection, ConnectionType, CopiedTransferItem, File, InsertBlob,
        InsertFile, InsertMaterialisation, InsertRepositoryName, ObservedBlob, Repository,
        RepositoryName,
    };
    use crate::utils::flow::{ExtFlow, Flow};
    use chrono::Utc;
    use futures::StreamExt;
    use futures::stream;
    use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};
    use uuid::Uuid;

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

        let name = db
            .lookup_current_repository_name(repo.repo_id.clone())
            .await
            .expect("lookup failed");
        assert!(name.is_none(), "Expected no name initially");

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

        let repo_initial = Repository {
            repo_id: repo_id.clone(),
            last_file_index: 0,
            last_blob_index: 0,
            last_name_index: 0,
        };
        db.merge_repositories(stream::iter([repo_initial]))
            .await
            .expect("initial merge failed");

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
        let remote_blob = InsertBlob {
            repo_id: "somewhere_else".into(),
            blob_id: "missing_blob".into(),
            blob_size: 50,
            has_blob: true,
            path: None,
            valid_from: now,
        };
        db.add_blobs(stream::iter([avail_blob, remote_blob]))
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

        let mat_blob = InsertBlob {
            repo_id: "somewhere".into(),
            blob_id: "mat_blob".into(),
            blob_size: 50,
            has_blob: true,
            path: None,
            valid_from: now,
        };
        db.add_blobs(stream::iter([mat_blob]))
            .await
            .expect("failed");

        let mat_file = InsertFile {
            path: "mat_file.txt".into(),
            blob_id: Some("mat_blob".into()),
            valid_from: now,
        };
        db.add_files(stream::iter([mat_file]))
            .await
            .expect("failed");

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
        db.truncate_virtual_filesystem()
            .await
            .expect("truncate_virtual_filesystem failed");

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

        let mut missing_stream = db.select_missing_files_on_virtual_filesystem(0).await;
        while let Some(_mf) = missing_stream.next().await {}
    }

    #[tokio::test]
    async fn test_connection_methods() {
        let db = setup_test_db().await;
        use Connection;

        let list = db.list_all_connections().await.expect("list failed");
        assert!(list.is_empty(), "Expected no connections initially");

        let conn = Connection {
            name: "conn1".into(),
            connection_type: ConnectionType::Local,
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
            blob_id: "1234567890".into(),
            blob_size: 50,
            has_blob: true,
            path: None,
            valid_from: now,
        };
        db.add_blobs(stream::iter([remote_blob]))
            .await
            .expect("failed to add remote blob");

        let file = InsertFile {
            path: "path.txt".into(),
            blob_id: Some("1234567890".into()),
            valid_from: now,
        };
        db.add_files(stream::iter([file]))
            .await
            .expect("failed to add file");

        let transfer_id = 123;
        let mut populate_stream = db
            .populate_missing_blobs_for_transfer(transfer_id, "remote_repo".into(), vec![])
            .await;
        let mut populated = Vec::new();
        while let Some(item) = populate_stream.next().await {
            populated.push(item.expect("failed to populate missing blobs"));
        }
        assert!(
            !populated.is_empty(),
            "Expected at least one missing blob transfer item"
        );
        let copied_stream = stream::iter([CopiedTransferItem {
            transfer_id,
            path: "12/34/567890".into(),
        }]);

        let mut select_stream = db.select_blobs_transfer(copied_stream).await;
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
            blob_id: Some("1234567890".into()),
            valid_from: now,
        };
        db.add_files(stream::iter([file])).await.expect("failed");

        let remote_blob = InsertBlob {
            repo_id: "remote_repo".into(),
            blob_id: "1234567890".into(),
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
                vec![],
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
        let copied_stream = stream::iter([CopiedTransferItem {
            transfer_id,
            path: "missing_file.txt".into(),
        }]);

        let mut select_stream = db.select_files_transfer(copied_stream).await;
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

    #[tokio::test]
    async fn test_observe_blobs() {
        let db = setup_test_db().await;
        let now = Utc::now();
        let repo = db.get_or_create_current_repository().await.expect("failed");

        let seed_blob = InsertBlob {
            repo_id: repo.repo_id.clone(),
            blob_id: "test_blob".into(),
            blob_size: 123,
            has_blob: true,
            path: Some("test_blob.txt".into()),
            valid_from: now,
        };
        db.add_blobs(stream::iter(vec![seed_blob]))
            .await
            .expect("failed to add seed blob");

        let observed = ObservedBlob {
            repo_id: repo.repo_id.clone(),
            has_blob: true,
            path: "test_blob.txt".into(),
            valid_from: now,
        };
        let count = db
            .observe_blobs(stream::iter(vec![observed]))
            .await
            .expect("observe_blobs failed");
        assert!(count >= 1, "Expected at least one observed blob inserted");

        let mut avail_stream = db.available_blobs(repo.repo_id.clone());
        let mut found = false;
        while let Some(result) = avail_stream.next().await {
            let blob: AvailableBlob = result.expect("available_blobs query failed");
            if blob.blob_id == "test_blob" {
                found = true;
                break;
            }
        }
        assert!(found, "Observed blob 'test_blob' should be available");
    }

    #[tokio::test]
    async fn test_select_repositories() {
        let db = setup_test_db().await;

        let repo1 = Repository {
            repo_id: "repo1".into(),
            last_file_index: 1,
            last_blob_index: 2,
            last_name_index: 3,
        };
        let repo2 = Repository {
            repo_id: "repo2".into(),
            last_file_index: 10,
            last_blob_index: 20,
            last_name_index: 30,
        };
        db.merge_repositories(stream::iter(vec![repo1.clone(), repo2.clone()]))
            .await
            .expect("merge_repositories failed");

        let mut stream = db.select_repositories().await;
        let mut repos = Vec::new();
        while let Some(result) = stream.next().await {
            repos.push(result.expect("select_repositories failed"));
        }
        assert!(
            repos.iter().any(|r| r.repo_id == "repo1"),
            "repo1 should be present"
        );
        assert!(
            repos.iter().any(|r| r.repo_id == "repo2"),
            "repo2 should be present"
        );
    }

    #[tokio::test]
    async fn test_select_repository_names() {
        let db = setup_test_db().await;
        let now = Utc::now();

        let rn1 = InsertRepositoryName {
            repo_id: "repo1".into(),
            name: "Name One".into(),
            valid_from: now,
        };
        let rn2 = InsertRepositoryName {
            repo_id: "repo2".into(),
            name: "Name Two".into(),
            valid_from: now,
        };
        db.add_repository_names(stream::iter(vec![rn1, rn2]))
            .await
            .expect("add_repository_names failed");

        let mut stream = db.select_repository_names(-1).await;
        let mut names = Vec::new();
        while let Some(result) = stream.next().await {
            let rn: RepositoryName = result.expect("select_repository_names failed");
            names.push(rn.name);
        }
        assert!(
            names.contains(&"Name One".to_string()),
            "Expected 'Name One' in repository names"
        );
        assert!(
            names.contains(&"Name Two".to_string()),
            "Expected 'Name Two' in repository names"
        );
    }

    #[tokio::test]
    async fn test_merge_files() {
        let db = setup_test_db().await;
        let now = Utc::now();

        let file1 = File {
            uuid: Uuid::new_v4().to_string(),
            path: "file_a.txt".into(),
            blob_id: Some("blob_a".into()),
            valid_from: now,
        };
        let file2 = File {
            uuid: Uuid::new_v4().to_string(),
            path: "file_b.txt".into(),
            blob_id: Some("blob_b".into()),
            valid_from: now,
        };
        db.merge_files(stream::iter(vec![file1.clone(), file2.clone()]))
            .await
            .expect("merge_files failed");

        let mut stream = db.select_files(-1).await;
        let mut paths = Vec::new();
        while let Some(result) = stream.next().await {
            let file: File = result.expect("select_files failed");
            paths.push(file.path);
        }
        assert!(
            paths.contains(&"file_a.txt".to_string()),
            "Expected file_a.txt to be merged"
        );
        assert!(
            paths.contains(&"file_b.txt".to_string()),
            "Expected file_b.txt to be merged"
        );
    }

    #[tokio::test]
    async fn test_merge_blobs() {
        let db = setup_test_db().await;
        let now = Utc::now();
        let repo = db.get_or_create_current_repository().await.expect("failed");

        let blob1 = Blob {
            uuid: Uuid::new_v4().to_string(),
            repo_id: repo.repo_id.clone(),
            blob_id: "merge_blob1".into(),
            blob_size: 111,
            has_blob: true,
            path: Some("path1".into()),
            valid_from: now,
        };
        let blob2 = Blob {
            uuid: Uuid::new_v4().to_string(),
            repo_id: repo.repo_id.clone(),
            blob_id: "merge_blob2".into(),
            blob_size: 222,
            has_blob: false,
            path: Some("path2".into()),
            valid_from: now,
        };
        db.merge_blobs(stream::iter(vec![blob1.clone(), blob2.clone()]))
            .await
            .expect("merge_blobs failed");

        let mut stream = db.select_blobs(-1).await;
        let mut blob_ids = Vec::new();
        while let Some(result) = stream.next().await {
            let blob: Blob = result.expect("select_blobs failed");
            blob_ids.push(blob.blob_id);
        }
        assert!(
            blob_ids.contains(&"merge_blob1".to_string()),
            "Expected merge_blob1 to be merged"
        );
        assert!(
            blob_ids.contains(&"merge_blob2".to_string()),
            "Expected merge_blob2 to be merged"
        );
    }

    #[tokio::test]
    async fn test_merge_repository_names() {
        let db = setup_test_db().await;
        let now = Utc::now();
        let rn1 = RepositoryName {
            uuid: Uuid::new_v4().to_string(),
            repo_id: "repo_merge".into(),
            name: "Merge Name 1".into(),
            valid_from: now,
        };
        let rn2 = RepositoryName {
            uuid: Uuid::new_v4().to_string(),
            repo_id: "repo_merge".into(),
            name: "Merge Name 2".into(),
            valid_from: now,
        };
        db.merge_repository_names(stream::iter(vec![rn1.clone(), rn2.clone()]))
            .await
            .expect("merge_repository_names failed");

        let mut stream = db.select_repository_names(-1).await;
        let mut names = Vec::new();
        while let Some(result) = stream.next().await {
            let rn: RepositoryName = result.expect("select_repository_names failed");
            names.push(rn.name);
        }

        assert!(
            names.contains(&"Merge Name 1".to_string())
                || names.contains(&"Merge Name 2".to_string()),
            "Expected one of the merged repository names to be present"
        );
    }

    #[tokio::test]
    async fn test_merge_files_deduplication() {
        let db = setup_test_db().await;
        let now = Utc::now();
        let fixed_uuid = "fixed-uuid-file";
        let file1 = File {
            uuid: fixed_uuid.to_string(),
            path: "dup_file.txt".into(),
            blob_id: Some("blob1".into()),
            valid_from: now,
        };
        let file2 = File {
            uuid: fixed_uuid.to_string(),
            path: "dup_file.txt".into(),
            blob_id: Some("blob2".into()),
            valid_from: now,
        };

        db.merge_files(stream::iter(vec![file1, file2]))
            .await
            .expect("merge_files failed");

        let mut stream = db.select_files(-1).await;
        let mut count = 0;
        let mut found_blob = None;
        while let Some(result) = stream.next().await {
            let file: File = result.expect("select_files failed");
            if file.uuid == fixed_uuid {
                count += 1;
                found_blob = file.blob_id;
            }
        }
        assert_eq!(count, 1, "Expected one deduplicated file record");
        assert_eq!(found_blob, Some("blob1".into()));
    }

    #[tokio::test]
    async fn test_merge_blobs_deduplication() {
        let db = setup_test_db().await;
        let now = Utc::now();
        let repo = db.get_or_create_current_repository().await.expect("failed");

        let fixed_uuid = "fixed-uuid-blob";
        let blob1 = Blob {
            uuid: fixed_uuid.to_string(),
            repo_id: repo.repo_id.clone(),
            blob_id: "dup_blob".into(),
            blob_size: 100,
            has_blob: true,
            path: Some("path1".into()),
            valid_from: now,
        };
        let blob2 = Blob {
            uuid: fixed_uuid.to_string(),
            repo_id: repo.repo_id.clone(),
            blob_id: "dup_blob".into(),
            blob_size: 200,
            has_blob: false,
            path: Some("path2".into()),
            valid_from: now,
        };

        db.merge_blobs(stream::iter(vec![blob1, blob2]))
            .await
            .expect("merge_blobs failed");

        let mut stream = db.select_blobs(-1).await;
        let mut count = 0;
        let mut found_size = None;
        while let Some(result) = stream.next().await {
            let blob: Blob = result.expect("select_blobs failed");
            if blob.uuid == fixed_uuid {
                count += 1;
                found_size = Some(blob.blob_size);
            }
        }
        assert_eq!(count, 1, "Expected one deduplicated blob record");
        assert_eq!(found_size, Some(100));
    }

    #[tokio::test]
    async fn test_merge_repository_names_deduplication() {
        let db = setup_test_db().await;
        let now = Utc::now();
        let fixed_uuid = "fixed-uuid-repo-name";
        let rn1 = RepositoryName {
            uuid: fixed_uuid.to_string(),
            repo_id: "repo_merge".into(),
            name: "First Name".into(),
            valid_from: now,
        };
        let rn2 = RepositoryName {
            uuid: fixed_uuid.to_string(),
            repo_id: "repo_merge".into(),
            name: "Second Name".into(),
            valid_from: now,
        };

        db.merge_repository_names(stream::iter(vec![rn1, rn2]))
            .await
            .expect("merge_repository_names failed");

        let mut stream = db.select_repository_names(-1).await;
        let mut count = 0;
        let mut found_name = None;
        while let Some(result) = stream.next().await {
            let rn: RepositoryName = result.expect("select_repository_names failed");
            if rn.uuid == fixed_uuid {
                count += 1;
                found_name = Some(rn.name);
            }
        }
        assert_eq!(count, 1, "Expected one deduplicated repository name record");
        assert_eq!(found_name, Some("First Name".into()));
    }

    #[tokio::test]
    async fn test_clean_transfers() {
        let db = setup_test_db().await;
        let now = Utc::now();

        let remote_blob = InsertBlob {
            repo_id: "remote_repo".into(),
            blob_id: "transfer_blob".into(),
            blob_size: 100,
            has_blob: true,
            path: Some("remote/path.txt".into()),
            valid_from: now,
        };
        db.add_blobs(stream::iter(vec![remote_blob]))
            .await
            .expect("failed to add remote blob");

        let transfer_id = 999;

        let mut populate_stream = db
            .populate_missing_blobs_for_transfer(transfer_id, "remote_repo".into(), vec![])
            .await;
        while let Some(_item) = populate_stream.next().await {}
        let copied_stream = stream::iter([CopiedTransferItem {
            transfer_id,
            path: "remote/path.txt".into(),
        }]);

        db.clean().await.unwrap();

        let mut select_stream = db.select_blobs_transfer(copied_stream).await;
        let mut count = 0;
        while let Some(item) = select_stream.next().await {
            let _ = item.expect("select_blobs_transfer failed");
            count += 1;
        }
        assert_eq!(count, 0, "Expected transfers to be cleaned");
    }
}
