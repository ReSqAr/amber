mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_resilience_missing_files() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"

        # action 1
        @a amber add
        @a redb clear_latest_filesystem
        
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
