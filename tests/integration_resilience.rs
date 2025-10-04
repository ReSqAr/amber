use serial_test::serial;

mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_resilience_missing_blobs() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"

        # action 1
        @a amber add
        @a sql 'DELETE FROM blobs'
        @a vfs reset
        
        # check 1
        @a amber status
        assert_output_contains "detected 1 new files"
        @a amber fsck
        assert_output_contains "found no altered and no incomplete files"
        
        # action 2
        @a amber add
        
        # check 2
        @a amber status
        assert_output_contains "detected 1 materialised files"
        @a amber fsck
        assert_output_contains "found no altered and no incomplete files"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_resilience_missing_files() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"

        # action 1
        @a amber add
        @a sql 'DELETE FROM files'
        @a vfs reset
        
        # check 1
        @a amber status
        @a amber fsck
        
        # action 2
        @a amber add
        
        # check 2 - TODO: this looks like the file was deleted in a remote
        @a amber status
        assert_output_contains "detected 1 materialised files"
        @a amber fsck
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_resilience_missing_materialisations() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"

        # action 1
        @a amber add
        @a sql 'DELETE FROM materialisations'
        @a vfs reset
        
        # check 1
        @a amber status
        assert_output_contains "detected 1 incomplete files"
        @a amber fsck
        assert_output_contains "detected 0 altered files and 1 incomplete files"
        
        # action 2
        @a amber add
        
        # check 2
        @a amber status
        assert_output_contains "detected 1 materialised files"
        @a amber fsck
        assert_output_contains "found no altered and no incomplete files"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_resilience_missing_materialisations_sync() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"

        # action 1
        @a amber add
        @a sql 'DELETE FROM materialisations'
        @a vfs reset
        
        # check 1
        @a amber status
        assert_output_contains "detected 1 incomplete files"
        @a amber fsck
        assert_output_contains "detected 0 altered files and 1 incomplete files"
        
        # action 2
        @a amber sync
        
        # check 2
        @a amber status
        assert_output_contains "detected 1 materialised files"
        @a amber fsck
        assert_output_contains "found no altered and no incomplete files"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_resilience_missing_materialisations_sync_broken_hardlink()
-> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"

        # action 1
        @a amber add
        @a sql 'DELETE FROM materialisations'
        @a vfs reset
        # break hardlink
        @a remove_file test.txt
        @a write_file test.txt "Hello world!"

        # check 1
        @a amber status
        assert_output_contains "detected 1 incomplete files"
        @a amber fsck
        assert_output_contains "detected 0 altered files and 1 incomplete files"
        
        # action 2
        @a amber sync
        
        # check 2
        @a amber status
        assert_output_contains "detected 1 materialised files"
        @a amber fsck
        assert_output_contains "found no altered and no incomplete files"
    "#;
    dsl_definition::run_dsl_script(script).await
}
