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
