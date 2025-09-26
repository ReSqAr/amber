#[cfg(test)]
mod tests {
    use crate::db::database::Database;
    use crate::db::migrations::run_migrations;

    use crate::db::models::{
        AvailableBlob, Blob, BlobTransferItem, Connection, ConnectionType, CopiedTransferItem,
        File, FileCheck, FileSeen, FileTransferItem, InsertBlob, InsertFile, InsertMaterialisation,
        InsertRepositoryName, MissingFile, Observation, ObservedBlob, Repository, RepositoryName,
        VirtualFileState,
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

    #[tokio::test]
    async fn populate_missing_blobs_is_pull_only() {
        let db = setup_test_db().await;
        let now = Utc::now();

        db.add_files(stream::iter([InsertFile {
            path: "file.txt".into(),
            blob_id: Some("abcdef123456".into()),
            valid_from: now,
        }]))
        .await
        .expect("add_files");

        let blob_id = "abcdef123456";
        db.add_blobs(stream::iter([InsertBlob {
            repo_id: "remote_repo".into(),
            blob_id: "abcdef123456".into(),
            blob_size: 12,
            has_blob: true,
            path: Some(if blob_id.len() > 6 {
                format!("{}/{}/{}", &blob_id[0..2], &blob_id[2..4], &blob_id[4..])
            } else {
                blob_id.to_string()
            }),
            valid_from: now,
        }]))
        .await
        .expect("add remote");

        let mut stream_a = db
            .populate_missing_blobs_for_transfer(1, "remote_repo".into(), vec![])
            .await;
        let mut items_a: Vec<BlobTransferItem> = vec![];
        while let Some(it) = stream_a.next().await {
            items_a.push(it.expect("populate A"));
        }
        assert!(
            !items_a.is_empty(),
            "expected at least one transfer (pull semantics)"
        );

        let db = setup_test_db().await;
        let now = Utc::now();
        let _local_repo = db
            .get_or_create_current_repository()
            .await
            .expect("repo")
            .repo_id;

        db.add_files(stream::iter([InsertFile {
            path: "file.txt".into(),
            blob_id: Some("abcdef123456".into()),
            valid_from: now,
        }]))
        .await
        .expect("add_files");

        // local has it available
        db.add_blobs(stream::iter([InsertBlob {
            repo_id: _local_repo.clone(),
            blob_id: "abcdef123456".into(),
            blob_size: 12,
            has_blob: true,
            path: None,
            valid_from: now,
        }]))
        .await
        .expect("add local");

        let mut stream_b = db
            .populate_missing_blobs_for_transfer(2, "remote_repo".into(), vec![])
            .await;
        let mut items_b: Vec<BlobTransferItem> = vec![];
        while let Some(it) = stream_b.next().await {
            items_b.push(it.expect("populate B"));
        }
        assert!(
            items_b.is_empty(),
            "expected no items (function is pull-only)"
        );
    }

    #[tokio::test]
    async fn populate_missing_files_is_pull_only_and_respects_path_selector() {
        let db = setup_test_db().await;
        let now = Utc::now();
        let local_repo = db
            .get_or_create_current_repository()
            .await
            .expect("repo")
            .repo_id;

        db.add_files(stream::iter([
            InsertFile {
                path: "dir1/a.txt".into(),
                blob_id: Some("aaa111".into()),
                valid_from: now,
            },
            InsertFile {
                path: "dir2/b.txt".into(),
                blob_id: Some("bbb222".into()),
                valid_from: now,
            },
        ]))
        .await
        .expect("add_files");

        let blob_id = "bbb222";
        let blob_id1 = "aaa111";
        db.add_blobs(stream::iter([
            InsertBlob {
                repo_id: "remote_repo".into(),
                blob_id: "aaa111".into(),
                blob_size: 10,
                has_blob: true,
                path: Some(if blob_id1.len() > 6 {
                    format!("{}/{}/{}", &blob_id1[0..2], &blob_id1[2..4], &blob_id1[4..])
                } else {
                    blob_id1.to_string()
                }),
                valid_from: now,
            },
            InsertBlob {
                repo_id: "remote_repo".into(),
                blob_id: "bbb222".into(),
                blob_size: 20,
                has_blob: true,
                path: Some(if blob_id.len() > 6 {
                    format!("{}/{}/{}", &blob_id[0..2], &blob_id[2..4], &blob_id[4..])
                } else {
                    blob_id.to_string()
                }),
                valid_from: now,
            },
        ]))
        .await
        .expect("add remote blobs");

        let mut s_all = db
            .populate_missing_files_for_transfer(
                7,
                local_repo.clone(),
                "remote_repo".into(),
                vec![],
            )
            .await;
        let mut all: Vec<FileTransferItem> = vec![];
        while let Some(it) = s_all.next().await {
            all.push(it.expect("populate all"));
        }
        assert_eq!(all.len(), 2, "expected both files to be proposed in pull");

        let mut s_sel = db
            .populate_missing_files_for_transfer(
                8,
                local_repo.clone(),
                "remote_repo".into(),
                vec!["dir1".into()],
            )
            .await;
        let mut only_dir1: Vec<FileTransferItem> = vec![];
        while let Some(it) = s_sel.next().await {
            only_dir1.push(it.expect("populate dir1"));
        }
        assert_eq!(only_dir1.len(), 1, "selector should filter to one file");
        assert_eq!(only_dir1[0].path, "dir1/a.txt");
    }

    #[tokio::test]
    async fn remote_assimilation_makes_missing_empty() {
        let db = setup_test_db().await;
        let now = Utc::now();

        db.add_files(stream::iter([InsertFile {
            path: "test.txt".into(),
            blob_id: Some("1234567890".into()),
            valid_from: now,
        }]))
        .await
        .expect("add_files");

        let blob_id = "1234567890";
        db.add_blobs(stream::iter([InsertBlob {
            repo_id: "remote_repo".into(),
            blob_id: "1234567890".into(),
            blob_size: 50,
            has_blob: true,
            path: Some(if blob_id.len() > 6 {
                format!("{}/{}/{}", &blob_id[0..2], &blob_id[2..4], &blob_id[4..])
            } else {
                blob_id.to_string()
            }),
            valid_from: now,
        }]))
        .await
        .expect("assimilate remote");

        let mut miss_stream = db.missing_blobs("remote_repo".into());
        let mut miss_items = vec![];
        while let Some(it) = miss_stream.next().await {
            miss_items.push(it.expect("missing"));
        }
        assert!(
            miss_items.is_empty(),
            "after assimilation, remote should have no missing blobs"
        );
    }

    #[tokio::test]
    async fn hashed_path_roundtrip_consistency() {
        let db = setup_test_db().await;
        let now = Utc::now();

        db.add_files(stream::iter([InsertFile {
            path: "x/y/z.txt".into(),
            blob_id: Some("cafebabedeadbeef".into()),
            valid_from: now,
        }]))
        .await
        .expect("add_files");

        let blob_id = "cafebabedeadbeef";
        let hp = if blob_id.len() > 6 {
            format!("{}/{}/{}", &blob_id[0..2], &blob_id[2..4], &blob_id[4..])
        } else {
            blob_id.to_string()
        };
        db.add_blobs(stream::iter([InsertBlob {
            repo_id: "remote_repo".into(),
            blob_id: "cafebabedeadbeef".into(),
            blob_size: 1234,
            has_blob: true,
            path: Some(hp.clone()),
            valid_from: now,
        }]))
        .await
        .expect("add remote availability");

        let mut avail = db.available_blobs("remote_repo".into());
        let mut seen = vec![];
        while let Some(it) = avail.next().await {
            seen.push(it.expect("available"));
        }
        assert!(
            seen.iter().any(|b| b.blob_id == "cafebabedeadbeef"),
            "remote should report the assimilated blob as available"
        );
    }

    #[tokio::test]
    async fn sync_marks_missing_then_clears_only_after_materialise_and_seen_id_matches() {
        use chrono::Utc;
        use futures::{StreamExt, stream};

        let db = setup_test_db().await;
        let now = Utc::now();
        let scan_id: i64 = 7;

        let repo_id = db.get_or_create_current_repository().await.unwrap().repo_id;
        let blob_id = "1234567890abcdef";

        db.add_files(stream::iter([InsertFile {
            path: "test.txt".into(),
            blob_id: Some(blob_id.into()),
            valid_from: now,
        }]))
        .await
        .unwrap();

        db.add_blobs(stream::iter([InsertBlob {
            repo_id: repo_id.clone(),
            blob_id: blob_id.into(),
            blob_size: 42,
            has_blob: true,
            path: Some({
                fn hashed(id: &str) -> String {
                    if id.len() > 6 {
                        format!("{}/{}/{}", &id[0..2], &id[2..4], &id[4..])
                    } else {
                        id.to_string()
                    }
                }
                hashed(blob_id)
            }),
            valid_from: now,
        }]))
        .await
        .unwrap();

        db.refresh_virtual_filesystem().await.unwrap();

        let mut s1 = db.select_missing_files_on_virtual_filesystem(scan_id).await;
        let mut missing1 = vec![];
        while let Some(r) = s1.next().await {
            missing1.push(r.unwrap());
        }
        assert_eq!(missing1.len(), 1);
        assert_eq!(missing1[0].path, "test.txt");
        assert_eq!(missing1[0].target_blob_id, blob_id);
        assert!(missing1[0].local_has_target_blob);

        db.add_materialisations(stream::iter([InsertMaterialisation {
            path: "test.txt".into(),
            blob_id: Some(blob_id.into()),
            valid_from: now,
        }]))
        .await
        .unwrap();
        db.refresh_virtual_filesystem().await.unwrap();

        let mut s2 = db.select_missing_files_on_virtual_filesystem(scan_id).await;
        let mut missing2 = vec![];
        while let Some(r) = s2.next().await {
            missing2.push(r.unwrap());
        }
        assert_eq!(
            missing2.len(),
            1,
            "materialisation alone does not clear the queue"
        );

        let obs = vec![Flow::Data(Observation::FileSeen(FileSeen {
            path: "test.txt".into(),
            seen_id: scan_id,
            seen_dttm: now.timestamp(),
            last_modified_dttm: now.timestamp(),
            size: 42,
        }))];
        let mut out = db
            .add_virtual_filesystem_observations(stream::iter(obs))
            .await;
        while let Some(_batch) = out.next().await { /* drive it */ }

        let mut s3 = db.select_missing_files_on_virtual_filesystem(scan_id).await;
        let mut missing3 = vec![];
        while let Some(r) = s3.next().await {
            missing3.push(r.unwrap());
        }
        assert!(
            missing3.is_empty(),
            "clears only after fs_last_seen_id == scan_id"
        );
    }

    #[tokio::test]
    async fn remote_assimilation_eliminates_missing() {
        let db = setup_test_db().await;
        let now = Utc::now();
        let remote_repo = db
            .get_or_create_current_repository()
            .await
            .expect("repo")
            .repo_id;

        let blob_id = "feedfacecafebeef";
        db.add_files(stream::iter([InsertFile {
            path: "hello.txt".into(),
            blob_id: Some(blob_id.into()),
            valid_from: now,
        }]))
        .await
        .expect("add_files");

        db.add_blobs(stream::iter([InsertBlob {
            repo_id: remote_repo.clone(),
            blob_id: blob_id.into(),
            blob_size: 123,
            has_blob: true,
            path: Some(if blob_id.len() > 6 {
                format!("{}/{}/{}", &blob_id[0..2], &blob_id[2..4], &blob_id[4..])
            } else {
                blob_id.to_string()
            }),
            valid_from: now,
        }]))
        .await
        .expect("add_blobs");

        let mut missing = db.missing_blobs(remote_repo.clone());
        let mut miss_items = vec![];
        while let Some(it) = missing.next().await {
            miss_items.push(it.expect("missing_blobs"));
        }
        assert!(
            miss_items.is_empty(),
            "with both file mapping and available blob, remote has no missing blobs"
        );

        let mut avail = db.available_blobs(remote_repo.clone());
        let mut avail_items: Vec<AvailableBlob> = vec![];
        while let Some(it) = avail.next().await {
            avail_items.push(it.expect("available_blobs"));
        }
        assert!(
            avail_items.iter().any(|b| b.blob_id == blob_id),
            "assimilated blob should be reported as available"
        );
    }

    #[tokio::test]
    async fn vfs_state_transitions_ok_materialisation_missing_to_ok() {
        let db = setup_test_db().await;
        let now = Utc::now();
        let remote_repo = db
            .get_or_create_current_repository()
            .await
            .expect("repo")
            .repo_id;

        let blob_id = "aa11bb22cc33";

        db.add_files(stream::iter([InsertFile {
            path: "state.txt".into(),
            blob_id: Some(blob_id.into()),
            valid_from: now,
        }]))
        .await
        .expect("add_files");

        db.add_blobs(stream::iter([InsertBlob {
            repo_id: remote_repo.clone(),
            blob_id: blob_id.into(),
            blob_size: 77,
            has_blob: true,
            path: Some(if blob_id.len() > 6 {
                format!("{}/{}/{}", &blob_id[0..2], &blob_id[2..4], &blob_id[4..])
            } else {
                blob_id.to_string()
            }),
            valid_from: now,
        }]))
        .await
        .expect("add_blobs");

        db.refresh_virtual_filesystem().await.expect("refresh");
        let checks = vec![
            Observation::FileSeen(FileSeen {
                path: "state.txt".into(),
                seen_id: 1,
                seen_dttm: now.timestamp(),
                last_modified_dttm: now.timestamp(),
                size: 77,
            }),
            Observation::FileCheck(FileCheck {
                path: "state.txt".into(),
                check_dttm: now.timestamp(),
                hash: blob_id.into(),
            }),
        ];

        let mut out = db
            .add_virtual_filesystem_observations(stream::iter(checks).map(Flow::Data))
            .await;
        let mut state1: Option<VirtualFileState> = None;
        while let Some(batch) = out.next().await {
            match batch {
                ExtFlow::Data(Ok(v)) | ExtFlow::Shutdown(Ok(v)) => {
                    for vf in v {
                        if vf.path == "state.txt" {
                            state1 = Some(vf.state);
                        }
                    }
                }
                ExtFlow::Data(Err(e)) | ExtFlow::Shutdown(Err(e)) => {
                    panic!("obs failed: {e}");
                }
            }
        }
        assert_eq!(
            state1,
            Some(VirtualFileState::OkMaterialisationMissing),
            "before recording materialisation, state should be ok_materialisation_missing"
        );

        db.add_materialisations(stream::iter([InsertMaterialisation {
            path: "state.txt".into(),
            blob_id: Some(blob_id.into()),
            valid_from: now,
        }]))
        .await
        .expect("add_materialisations");

        db.refresh_virtual_filesystem().await.expect("refresh 2");

        let checks2 = vec![
            Observation::FileSeen(FileSeen {
                path: "state.txt".into(),
                seen_id: 2,
                seen_dttm: now.timestamp(),
                last_modified_dttm: now.timestamp(),
                size: 77,
            }),
            Observation::FileCheck(FileCheck {
                path: "state.txt".into(),
                check_dttm: now.timestamp(),
                hash: blob_id.into(),
            }),
        ];

        let mut out2 = db
            .add_virtual_filesystem_observations(stream::iter(checks2).map(Flow::Data))
            .await;
        let mut state2: Option<VirtualFileState> = None;
        while let Some(batch) = out2.next().await {
            match batch {
                ExtFlow::Data(Ok(v)) | ExtFlow::Shutdown(Ok(v)) => {
                    for vf in v {
                        if vf.path == "state.txt" {
                            state2 = Some(vf.state);
                        }
                    }
                }
                ExtFlow::Data(Err(e)) | ExtFlow::Shutdown(Err(e)) => {
                    panic!("obs failed: {e}");
                }
            }
        }
        assert_eq!(
            state2,
            Some(VirtualFileState::Ok),
            "after recording materialisation, state should be ok"
        );
    }

    #[tokio::test]
    async fn file_then_blob_produces_materialise_candidate() {
        let db: Database = setup_test_db().await;
        let now = Utc::now();
        let blob_id = "abcdeffedcba1234";

        let hashed_blob_path = |id: &str| format!("{}/{}/{}", &id[0..2], &id[2..4], &id[4..]);

        let collect_missing = |scan_id: i64| {
            let db = &db;
            async move {
                let mut s = db.select_missing_files_on_virtual_filesystem(scan_id).await;
                let mut v: Vec<MissingFile> = vec![];
                while let Some(r) = s.next().await {
                    v.push(r.expect("select_missing_files_on_virtual_filesystem"));
                }
                v
            }
        };

        db.add_files(stream::iter([InsertFile {
            path: "test.txt".into(),
            blob_id: Some(blob_id.into()),
            valid_from: now,
        }]))
        .await
        .expect("add_files");

        db.refresh_virtual_filesystem().await.expect("refresh vfs");
        let miss0 = collect_missing(0).await;
        assert!(miss0.is_empty(), "no candidate until the blob exists");

        let repo_id = db.get_or_create_current_repository().await.unwrap().repo_id;
        db.add_blobs(stream::iter([InsertBlob {
            repo_id,
            blob_id: blob_id.into(),
            blob_size: 42,
            has_blob: true,
            path: Some(hashed_blob_path(blob_id)),
            valid_from: now,
        }]))
        .await
        .expect("add_blobs");

        db.refresh_virtual_filesystem()
            .await
            .expect("refresh vfs 2");
        let miss1 = collect_missing(0).await;
        assert_eq!(
            miss1.len(),
            1,
            "exactly one file should require materialisation now"
        );
        assert_eq!(miss1[0].path, "test.txt");
        assert_eq!(miss1[0].target_blob_id, blob_id);
    }

    #[tokio::test]
    async fn blob_then_file_produces_materialise_candidate() {
        let db: Database = setup_test_db().await;
        let now = Utc::now();
        let blob_id = "0123456789abcdef";

        let hashed_blob_path = |id: &str| format!("{}/{}/{}", &id[0..2], &id[2..4], &id[4..]);
        let collect_missing = |scan_id: i64| {
            let db = &db;
            async move {
                let mut s = db.select_missing_files_on_virtual_filesystem(scan_id).await;
                let mut v = vec![];
                while let Some(r) = s.next().await {
                    v.push(r.expect("select_missing_files"));
                }
                v
            }
        };

        let repo_id = db.get_or_create_current_repository().await.unwrap().repo_id;

        db.add_blobs(stream::iter([InsertBlob {
            repo_id,
            blob_id: blob_id.into(),
            blob_size: 77,
            has_blob: true,
            path: Some(hashed_blob_path(blob_id)),
            valid_from: now,
        }]))
        .await
        .expect("add_blobs");

        db.add_files(stream::iter([InsertFile {
            path: "hello.txt".into(),
            blob_id: Some(blob_id.into()),
            valid_from: now,
        }]))
        .await
        .expect("add_files");

        db.refresh_virtual_filesystem().await.expect("refresh vfs");
        let miss = collect_missing(0).await;
        assert_eq!(miss.len(), 1, "should require materialisation");
        assert_eq!(miss[0].path, "hello.txt");
        assert_eq!(miss[0].target_blob_id, blob_id);
    }

    #[tokio::test]
    async fn available_from_other_repo_still_requires_materialisation() {
        let db: Database = setup_test_db().await;
        let now = Utc::now();
        let blob_id = "feedfacecafebeef";

        let hashed_blob_path = |id: &str| format!("{}/{}/{}", &id[0..2], &id[2..4], &id[4..]);
        let collect_missing = |scan_id: i64| {
            let db = &db;
            async move {
                let mut s = db.select_missing_files_on_virtual_filesystem(scan_id).await;
                let mut v = vec![];
                while let Some(r) = s.next().await {
                    v.push(r.expect("select_missing_files"));
                }
                v
            }
        };

        db.add_files(stream::iter([InsertFile {
            path: "cross.txt".into(),
            blob_id: Some(blob_id.into()),
            valid_from: now,
        }]))
        .await
        .expect("add_files");

        db.add_blobs(stream::iter([InsertBlob {
            repo_id: "some-other-repo-id".to_string(),
            blob_id: blob_id.into(),
            blob_size: 9001,
            has_blob: true,
            path: Some(hashed_blob_path(blob_id)),
            valid_from: now,
        }]))
        .await
        .expect("add_blobs");

        db.refresh_virtual_filesystem().await.expect("refresh vfs");
        let miss = collect_missing(0).await;
        assert_eq!(
            miss.len(),
            1,
            "materialisation is still needed even if availability is from another repo"
        );
        assert_eq!(miss[0].path, "cross.txt");
        assert_eq!(miss[0].target_blob_id, blob_id);
        assert!(
            !miss[0].local_has_target_blob,
            "should reflect lack of local availability in the current repo"
        );
    }

    #[tokio::test]
    async fn clears_only_after_materialise_and_seen_id_matches() {
        let db: Database = setup_test_db().await;
        let now = Utc::now();
        let scan_id: i64 = 42;
        let blob_id = "aa11bb22cc33";

        let hashed_blob_path = |id: &str| format!("{}/{}/{}", &id[0..2], &id[2..4], &id[4..]);
        let collect_missing = |scan_id: i64| {
            let db = &db;
            async move {
                let mut s = db.select_missing_files_on_virtual_filesystem(scan_id).await;
                let mut v = vec![];
                while let Some(r) = s.next().await {
                    v.push(r.expect("select_missing_files"));
                }
                v
            }
        };

        let repo_id = db.get_or_create_current_repository().await.unwrap().repo_id;
        db.add_files(stream::iter([InsertFile {
            path: "state.txt".into(),
            blob_id: Some(blob_id.into()),
            valid_from: now,
        }]))
        .await
        .unwrap();
        db.add_blobs(stream::iter([InsertBlob {
            repo_id,
            blob_id: blob_id.into(),
            blob_size: 64,
            has_blob: true,
            path: Some(hashed_blob_path(blob_id)),
            valid_from: now,
        }]))
        .await
        .unwrap();

        db.refresh_virtual_filesystem().await.unwrap();
        let miss1 = collect_missing(scan_id).await;
        assert_eq!(miss1.len(), 1, "initially needs materialisation");

        db.add_materialisations(stream::iter([InsertMaterialisation {
            path: "state.txt".into(),
            blob_id: Some(blob_id.into()),
            valid_from: now,
        }]))
        .await
        .unwrap();
        db.refresh_virtual_filesystem().await.unwrap();

        let miss2 = collect_missing(scan_id).await;
        assert_eq!(miss2.len(), 1, "materialisation alone doesn't clear");

        let mut out = db
            .add_virtual_filesystem_observations(stream::iter([
                Flow::Data(Observation::FileSeen(FileSeen {
                    path: "state.txt".into(),
                    seen_id: scan_id,
                    seen_dttm: now.timestamp(),
                    last_modified_dttm: now.timestamp(),
                    size: 64,
                })),
                Flow::Data(Observation::FileCheck(FileCheck {
                    path: "state.txt".into(),
                    check_dttm: now.timestamp(),
                    hash: blob_id.into(),
                })),
            ]))
            .await;
        while let Some(_batch) = out.next().await { /* drain */ }

        let miss3 = collect_missing(scan_id).await;
        assert!(miss3.is_empty(), "clears after FileSeen matches scan id");
    }
}
