use serial_test::serial;
mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_sync_pull_push() -> Result<(), anyhow::Error> {
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
