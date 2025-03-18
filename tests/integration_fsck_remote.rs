use serial_test::serial;
mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rclone_repo_fsck() -> anyhow::Result<(), anyhow::Error> {
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
async fn integration_test_rclone_repo_fsck_missing_file_detection()
-> anyhow::Result<(), anyhow::Error> {
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
async fn integration_test_rclone_repo_fsck_corrupted_file_detection()
-> anyhow::Result<(), anyhow::Error> {
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
async fn integration_test_rclone_repo_fsck_no_files() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        @a amber remote add store rclone :local:/$ROOT/rclone

        # action
        @a amber fsck store
    "#;
    dsl_definition::run_dsl_script(script).await
}
