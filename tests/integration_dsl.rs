use anyhow::Result;
use serial_test::serial;

mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_auto_restore_removed_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "This file will be restored"
        @a assert_exists test.txt "This file will be restored"
        @a amber add
        @a remove_file test.txt
        @a assert_does_not_exist test.txt

        # action
        @a amber sync

        # then
        @a assert_exists test.txt "This file will be restored"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_deduplicate() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test-1.txt "I am not a snowflake"
        @a write_file test-2.txt "I am not a snowflake"

        # action
        @a amber add

        # then
        @a assert_hardlinked test-1.txt test-2.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_sync() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a random_file test-a.txt 100
        @a amber add

        @b amber init b
        @b write_file test-b.txt "Hello world!"
        @b amber add

        @a amber remote add b local $ROOT/b

        # action
        @a amber push b
        @a amber pull b
        @b amber sync

        # then
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rclone_repo() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello store!"
        @a amber add

        @b amber init b

        @a amber remote add b local $ROOT/b
        @a amber remote add store rclone :local:/$ROOT/rclone
        @b amber remote add store rclone :local:/$ROOT/rclone

        # action
        @a amber push store
        @a amber sync b
        @b amber pull store

        # then
        assert_equal a b
        @b assert_exists test.txt "Hello store!"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_push_path_selector() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file to-be-copied.txt "content to be transfered" 
        @a write_file not-transfered.txt "content only in a" 
        @a amber add

        @b amber init b

        @a amber remote add b local $ROOT/b

        # action
        @a amber push b to-be-copied.txt
        @b amber sync

        # then
        @b assert_exists to-be-copied.txt "content to be transfered"
        @b assert_does_not_exist not-transfered.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_pull_path_selector() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        @b amber init b
        @b write_file to-be-copied.txt "content to be transfered" 
        @b write_file not-transfered.txt "content only in a" 
        @b amber add

        @a amber remote add b local $ROOT/b

        # action
        @a amber pull b to-be-copied.txt

        # then
        @a assert_exists to-be-copied.txt "content to be transfered"
        @a assert_does_not_exist not-transfered.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_sync_same_filename_pull_push() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @b amber init b
        @a write_file test.txt "Hello A world!"
        @a amber add
        @b write_file test.txt "Hello B world!"
        @b amber add
        @a amber remote add b local $ROOT/b

        # action
        @a amber pull b
        @a amber push b
        @b amber sync

        # then
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_sync_same_filename_push_pull() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @b amber init b
        @a write_file test.txt "Hello world - I am A!"
        @a amber add
        @b write_file test.txt "Hello world - I am B!"
        @b amber add
        @a amber remote add b local $ROOT/b

        # action
        @a amber push b
        @a amber pull b
        @b amber sync

        # then
        @a assert_exists test.txt "Hello world - I am B!"
        @b assert_exists test.txt "Hello world - I am B!"
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_missing() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @b amber init b
        @a write_file test-a.txt "Hello A world!"
        @a amber add
        @b write_file test-b.txt "Hello B world!"
        @b amber add
        @a amber remote add b local $ROOT/b

        # action 1
        @a amber sync b
        
        # then
        @a amber missing
        assert_output_contains "missing test-b.txt (exists in: b)"
        @b amber missing
        assert_output_contains "missing test-a.txt (exists in: a)"

        # action 2
        @a amber pull b

        # then
        @a amber missing
        assert_output_contains "no files missing"
        @b amber missing
        assert_output_contains "missing test-a.txt (exists in: a)"

        # action 3
        @a amber push b

        # then
        @a amber missing
        assert_output_contains "no files missing"
        @b amber missing
        assert_output_contains "no files missing"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_status_missing() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @b amber init b
        @a write_file test-a.txt "Hello A world!"
        @a amber add
        @b write_file test-b.txt "Hello B world!"
        @b amber add
        @a amber remote add b local $ROOT/b

        # action 1
        @a amber sync b

        # then
        @a amber status
        assert_output_contains "missing test-b.txt"
        @b amber status
        assert_output_contains "missing test-a.txt"

        # action 2
        @a amber pull b

        # then
        @a amber status
        assert_output_contains "detected 2 materialised files"
        @b amber status
        assert_output_contains "missing test-a.txt"

        # action 3
        @a amber push b
        @b amber sync

        # then
        @a amber status
        assert_output_contains "detected 2 materialised files"
        @b amber status
        assert_output_contains "detected 2 materialised files"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_altered_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Original content"
        @a amber add
        # Simulate user alteration by overwriting the file with wrong content.
        @a remove_file test.txt
        @a write_file test.txt "User altered content"

        @a amber sync

        # action
        @a amber status

        # then: we do not overwrite the on-disk file but do alert the user
        assert_output_contains "altered test.txt"
        @a assert_exists test.txt "User altered content"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_outdated_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello A world!"
        @a amber add

        @b amber init b
        @b write_file test.txt "Hello B world!"
        @b amber add

        @a amber remote add b local $ROOT/b
        @a amber sync b

        # action
        @a amber status
        # then
        assert_output_contains "outdated test.txt"

        # action
        @a amber pull b
        @a amber push b
        @b amber sync
        # then
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_missing_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        @b amber init b
        @b write_file test.txt "Hello B world!"
        @b amber add

        @a amber remote add b local $ROOT/b
        @a amber sync b

        # action
        @a amber status
        # then
        assert_output_contains "missing test.txt"

        # action
        @a amber pull b
        @a amber push b
        @b amber sync
        # then
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_new_file_state() -> Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file new.txt "I am new"
        @a amber status
        assert_output_contains "new new.txt"
        @a assert_exists new.txt "I am new"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_delete_synced_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test-a.txt "Hello A world!"
        @a amber add

        @b amber init b
        @b write_file test-b.txt "Hello B world!"
        @b amber add

        @a amber remote add b local $ROOT/b
        @a amber sync b

        # action 1
        @a amber status
        # then
        assert_output_contains "missing test-b.txt"

        # action 2
        @b amber status
        # then
        assert_output_contains "missing test-a.txt"

        # when
        @a amber pull b
        @a amber status
        @b amber status

        # action 3
        @a amber remove test-a.txt test-b.txt
        @a amber sync b
        @b amber sync

        # then
        assert_equal a b
        @a assert_does_not_exist test-a.txt
        @a assert_does_not_exist test-b.txt
        @b assert_does_not_exist test-a.txt
        @b assert_does_not_exist test-b.txt

        @a amber status
        assert_output_contains "no files detected"
        @b amber status
        assert_output_contains "no files detected"

        @a amber missing
        assert_output_contains "no files missing"
        @b amber missing
        assert_output_contains "no files missing"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_fsck() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        # action
        @a amber fsck
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rclone_repo_fsck() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello store!"
        @a amber add

        @a amber remote add store rclone :local:/$ROOT/rclone
        @a amber push store

        # action
        @a amber fsck store
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rclone_repo_fsck_missing_file_detection() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello store!"
        @a amber add

        @a amber remote add store rclone :local:/$ROOT/rclone
        @a amber push store
        @a amber missing store
        assert_output_contains "no files missing"

        @rclone remove_file test.txt

        # action
        @a amber fsck store
        
        @a amber missing store
        assert_output_contains "missing test.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rclone_repo_fsck_corrupted_file_detection() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello store!"
        @a amber add

        @a amber remote add store rclone :local:/$ROOT/rclone
        @a amber push store
        @a amber missing store
        assert_output_contains "no files missing"

        @rclone write_file test.txt "CORRUPTED"

        # action
        @a amber fsck store
        
        @a amber missing store
        assert_output_contains "missing test.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rm() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        # action
        @a amber rm test.txt
        assert_output_contains "deleted test.txt"

        # then
        @a assert_does_not_exist test.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rm_soft() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        # action
        @a amber rm --soft test.txt
        assert_output_contains "deleted [soft] test.txt"

        # then
        @a assert_exists test.txt
        @a amber status
        assert_output_contains "new test.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rm_not_existing_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        # action
        @a amber rm does-not-exist
        assert_output_contains "already deleted"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_fs_mv() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        @b amber init b

        @a amber remote add b local $ROOT/b

        # action 1
        @a amber push b
        @b amber sync

        # then 1
        assert_equal a b

        # action 2
        @a amber mv test.txt test.moved
        @a amber sync b
        @b amber sync

        # then 2
        @a assert_exists test.moved "Hello world!"
        @b assert_exists test.moved "Hello world!"
        @a assert_does_not_exist test.txt
        @b assert_does_not_exist test.txt
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_config_set_name() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        # action
        @a amber config set-name b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_ssh_connection_list() -> Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a start_ssh 4567 hunter2

        @b amber init b

        @b amber remote add a-ssh ssh "user:hunter2@localhost:4567/"
        
        @b amber remote list
        assert_output_contains "a-ssh"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_ssh_connection_sync() -> Result<(), anyhow::Error> {
    let script = r#"
        # Set up repositories
        @a amber init a
        @a write_file test1.txt "Common content!"
        @a write_file test2.txt "Common content!"
        @a amber add

        @b amber init b
        @b write_file test1.txt "Common content!"
        @b amber add

        # Start SSH server for repository A
        @a start_ssh 4567 hunter2

        # Add remote via SSH connection
        @b amber remote add a-ssh ssh "user:hunter2@localhost:4567/"

        # Pull from SSH remote
        @b amber sync a-ssh

        # Verify the file was transferred
        @b assert_exists test2.txt "Common content!"
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_ssh_connection_rclone_pull() -> Result<(), anyhow::Error> {
    let script = r#"
        # Set up repositories
        @a amber init a
        @a write_file test.txt "Hello from repository A!"
        @a amber add

        @b amber init b

        # Start SSH server for repository A
        @a start_ssh 4567 hunter2

        # Add remote via SSH connection from B to A
        @b amber remote add a-ssh ssh "user:hunter2@localhost:4567/"

        # Pull from SSH remote A
        @b amber pull a-ssh

        # Verify the file was transferred
        @b assert_exists test.txt "Hello from repository A!"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_ssh_connection_rclone_push() -> Result<(), anyhow::Error> {
    let script = r#"
        # Set up repositories
        @a amber init a
        @a write_file test.txt "Hello from repository A!"
        @a amber add

        @b amber init b

        # Start SSH server for repository B
        @b start_ssh 4567 hunter2

        # Add remote via SSH connection from A to B
        @a amber remote add b-ssh ssh "user:hunter2@localhost:4567/"

        # Push from A to SSH remote B
        @a amber push b-ssh

        @b end_ssh
        @b amber sync

        # Verify the file was transferred
        @b assert_exists test.txt "Hello from repository A!"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_nested_dirs_and_skip_deduplication() -> Result<(), anyhow::Error> {
    let script = r#"
        # Initialize repositories
        @a amber init a
        @b amber init b
        
        # Create nested directory structures with files in repository A
        @a write_file dir1/file1.txt "Content in dir1/file1.txt"
        @a write_file dir1/file2.txt "Content in dir1/file2.txt"
        @a write_file dir1/subdir/file3.txt "Content in dir1/subdir/file3.txt"
        @a write_file dir2/file4.txt "Content in dir2/file4.txt"
        @a write_file dir2/file5.txt "Identical content"
        @a write_file dir2/file6.txt "Identical content"
        
        # Add with skip-deduplication flag to prevent hardlinking of identical files
        @a amber add --skip-deduplication
        
        # Verify files are not hardlinked despite having identical content
        @a amber status
        
        # Verify files with identical content are NOT hardlinked due to skip-deduplication
        @a assert_not_hardlinked dir2/file5.txt dir2/file6.txt
        
        # Connect repositories
        @a amber remote add b local $ROOT/b
        
        # Test selective sync with complex path pattern (only dir1)
        @a amber push b dir1
        @b amber sync
        
        # Verify dir1 and its contents were synced
        @b assert_exists dir1/file1.txt "Content in dir1/file1.txt"
        @b assert_exists dir1/file2.txt "Content in dir1/file2.txt"
        @b assert_exists dir1/subdir/file3.txt "Content in dir1/subdir/file3.txt"
        
        # Verify dir2 was not synced
        @b assert_does_not_exist dir2/file4.txt
        
        # Now sync everything
        @a amber push b
        @b amber sync
        
        # Verify all files are now present
        @b assert_exists dir2/file4.txt "Content in dir2/file4.txt"
        @b assert_exists dir2/file5.txt "Identical content"
        @b assert_exists dir2/file6.txt "Identical content"
        
        # Test that normal add DOES deduplicate files
        @b write_file new_file1.txt "Duplicate content"
        @b write_file new_file2.txt "Duplicate content"
        @b amber add
        
        # Verify these new files ARE hardlinked (default behavior)
        @b assert_hardlinked new_file1.txt new_file2.txt
        
        # Verify after full sync, repositories are identical
        @a amber pull b
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_fsck_quarantine_behavior() -> Result<(), anyhow::Error> {
    let script = r#"
        # Initialize repository
        @a amber init a
        
        # Create a file with known content (empty file has known hash)
        @a write_file empty.txt ""
        @a amber add
        
        # Verify file is tracked and not missing
        @a amber missing
        assert_output_contains "no files missing"
        
        # Locate the blob file for our empty file
        # The SHA-256 hash of an empty file is:
        # e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        @a assert_exists .amb/blobs/e3/b0/c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        
        # Corrupt the blob by overwriting it with different content
        @a write_file .amb/blobs/e3/b0/c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 "corrupted content"
        
        # Run fsck to detect the corruption (should move the corrupt file to quarantine)
        @a amber fsck
        
        # Check that fsck detected a corruption
        assert_output_contains "blob corrupted"
        
        # Check that the original blob no longer exists
        @a assert_does_not_exist .amb/blobs/e3/b0/c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        
        # Verify that a quarantine file exists
        # File name pattern is <actual_hash>.<timestamp>
        # We only check if the quarantine directory has any files in it
        @a amber status
        @a assert_exists .amb/quarantine
        
        # Now the file should be reported as missing
        @a amber missing
        assert_output_contains "missing empty.txt (lost - no known location)"
        assert_output_contains "detected 1 missing files and 1 missing blobs"
        
        # Status should also show the file as missing
        @a amber status
        assert_output_contains "altered empty.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}
