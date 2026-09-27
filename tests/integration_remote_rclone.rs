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

/// A store keeps a blob under the name it was uploaded with. A new file that
/// takes that name later must not overwrite it.
#[tokio::test(flavor = "multi_thread")]
async fn integration_test_rclone_push_keeps_a_blob_whose_name_is_reused_after_remove()
-> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file x.txt "old"
        @a write_file y.txt "old"
        @a amber add
        @a amber remote add store rclone :local:/$ROOT/rclone
        @a amber push store

        @a amber remove x.txt
        @a write_file x.txt "new"
        @a amber add

        # action
        @a amber push store

        # then
        @rclone assert_exists x.txt "old"

        # action
        @b amber init b
        @b amber remote add store rclone :local:/$ROOT/rclone
        @b amber pull store

        # then
        @b assert_exists x.txt "new"
        @b assert_exists y.txt "old"
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_rclone_push_keeps_a_blob_whose_name_is_reused_after_move()
-> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file a.txt "moved"
        @a amber add
        @a amber remote add store rclone :local:/$ROOT/rclone
        @a amber push store

        @a amber mv a.txt b.txt
        @a write_file a.txt "new"
        @a amber add

        # action
        @a amber push store

        # then
        @rclone assert_exists a.txt "moved"

        # action
        @b amber init b
        @b amber remote add store rclone :local:/$ROOT/rclone
        @b amber pull store

        # then
        @b assert_exists a.txt "new"
        @b assert_exists b.txt "moved"
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

/// Some targets hold names differing only in case as one file.
#[tokio::test(flavor = "multi_thread")]
async fn integration_test_rclone_push_names_differing_in_case_apart() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file Photo.JPG "upper"
        @a write_file photo.jpg "lower"
        @a amber add
        @a amber remote add store rclone :local:/$ROOT/rclone

        # action
        @a amber push store

        # then
        @rclone assert_exists Photo.JPG "upper"
        @rclone assert_does_not_exist photo.jpg

        # action
        @b amber init b
        @b amber remote add store rclone :local:/$ROOT/rclone
        @b amber pull store

        # then
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}
