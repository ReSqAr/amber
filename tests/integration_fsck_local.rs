use serial_test::serial;
mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_fsck() -> anyhow::Result<(), anyhow::Error> {
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
async fn integration_test_fsck_quarantine_behavior() -> anyhow::Result<(), anyhow::Error> {
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
