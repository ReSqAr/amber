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
async fn integration_test_fsck_quarantine_behavior_with_hard_links()
-> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        # Initialize repository
        @a amber --prefer-hard-links init a
        
        # Create a file with known content (empty file has known hash)
        @a write_file empty.txt ""
        @a amber --prefer-hard-links add
        
        # Verify file is tracked and not missing
        @a amber --prefer-hard-links missing
        assert_output_contains "no files missing"
        
        # Locate the blob file for our empty file
        # The Blake3 hash of an empty file is:
        # af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262
        @a assert_exists .amb/blobs/af/13/49b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262
        
        # Corrupt the blob by overwriting it with different content
        @a write_file .amb/blobs/af/13/49b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262 "corrupted content"
        
        # Run fsck to detect the corruption (should move the corrupt file to quarantine)
        @a amber --prefer-hard-links fsck
        
        # Check that fsck detected a corruption
        assert_output_contains "blob corrupted"
        
        # Check that the original blob no longer exists
        @a assert_does_not_exist .amb/blobs/af/13/49b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262
        
        # Verify that a quarantine file exists
        # File name pattern is <actual_hash>.<timestamp>
        # We only check if the quarantine directory has any files in it
        @a amber --prefer-hard-links status
        @a assert_exists .amb/quarantine
        
        # Now the file should be reported as missing
        @a amber --prefer-hard-links missing
        assert_output_contains "missing empty.txt (lost - no known location)"
        assert_output_contains "detected 1 missing files and 1 missing blobs"
        
        # Status should also show the file as missing
        @a amber --prefer-hard-links status
        assert_output_contains "altered empty.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_fsck_quarantine_behavior_with_ref_links()
-> anyhow::Result<(), anyhow::Error> {
    if !dsl_definition::capability_check_ref_link().await? {
        eprintln!("Skipping test: ref links are not supported");
        return Ok(());
    }

    let script = r#"
        # Initialize repository
        @a amber --prefer-ref-links init a
        
        # Create a file with known content (empty file has known hash)
        @a write_file empty.txt ""
        @a amber --prefer-ref-links add
        
        # Verify file is tracked and not missing
        @a amber --prefer-ref-links missing
        assert_output_contains "no files missing"
        
        # Locate the blob file for our empty file
        # The Blake3 hash of an empty file is:
        # af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262
        @a assert_exists .amb/blobs/af/13/49b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262
        
        # Corrupt the blob by overwriting it with different content
        @a write_file .amb/blobs/af/13/49b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262 "corrupted content"
        
        # Run fsck to detect the corruption (should move the corrupt file to quarantine)
        @a amber --prefer-ref-links fsck
        
        # Check that fsck detected a corruption
        assert_output_contains "blob corrupted"
        
        # Check that the original blob no longer exists
        @a assert_does_not_exist .amb/blobs/af/13/49b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262
        
        # Verify that a quarantine file exists
        # File name pattern is <actual_hash>.<timestamp>
        # We only check if the quarantine directory has any files in it
        @a amber --prefer-ref-links status
        @a assert_exists .amb/quarantine
        
        # Now the file should be reported as missing
        @a amber --prefer-ref-links missing
        assert_output_contains "missing empty.txt (lost - no known location)"
        assert_output_contains "detected 1 missing files and 1 missing blobs"
        
        # Status should also show the file as incomplete
        @a amber --prefer-ref-links status
        assert_output_contains "detected 1 incomplete files"
    "#;
    dsl_definition::run_dsl_script(script).await
}
