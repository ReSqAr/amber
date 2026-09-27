mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_rclone_repo_pull_push() -> Result<(), anyhow::Error> {
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
async fn integration_test_rclone_repo_pull_push_sync_via_exported_parquet_store()
-> Result<(), anyhow::Error> {
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
        @b amber pull store

        # then
        assert_equal a b
        @b assert_exists test.txt "Hello store!"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_rclone_sync_via_exported_parquet_store_and_missing()
-> Result<(), anyhow::Error> {
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
        @a amber sync store
        @b amber sync store

        # then
        @b amber missing
        assert_output_contains "missing test.txt (exists in: a)"

        # action
        @a amber push store
        @b amber sync store

        # then
        @b amber missing
        assert_output_contains "missing test.txt (exists in: a, store)"
    "#;
    dsl_definition::run_dsl_script(script).await
}

/// Files that share a blob need only one copy of it in the store.
#[tokio::test(flavor = "multi_thread")]
async fn integration_test_rclone_push_uploads_a_shared_blob_once() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file one.txt "same"
        @a write_file two.txt "same"
        @a amber add
        @a amber remote add store rclone :local:/$ROOT/rclone

        # action
        @a amber push store

        # then
        assert_output_contains "pushed 1 blobs"

        # action
        @b amber init b
        @b amber remote add store rclone :local:/$ROOT/rclone
        @b amber pull store

        # then
        assert_output_contains "pulled 1 blobs"
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}
