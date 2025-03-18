use serial_test::serial;

mod dsl_definition;

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
        @a sql 'DELETE FROM virtual_filesystem'
        
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
async fn integration_test_resilience_missing_blobs() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"

        # action 1
        @a amber add
        @a sql 'DELETE FROM blobs'
        @a sql 'DELETE FROM virtual_filesystem'
        
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
