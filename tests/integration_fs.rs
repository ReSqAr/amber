use serial_test::serial;
mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_mv() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        @b amber init b

        @a amber remote add b local $ROOT/b

        # action 1
        @a amber push b
        @b amber sync

        # then 1
        assert_equal a b

        # action 2
        @a amber mv test.txt test.moved
        @a amber sync b
        @b amber sync

        # then 2
        @a assert_exists test.moved "Hello world!"
        @b assert_exists test.moved "Hello world!"
        @a assert_does_not_exist test.txt
        @b assert_does_not_exist test.txt
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rm() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        # action
        @a amber rm test.txt
        assert_output_contains "deleted test.txt"

        # then
        @a assert_does_not_exist test.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rm_soft() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        # action
        @a amber rm --soft test.txt
        assert_output_contains "deleted [soft] test.txt"

        # then
        @a assert_exists test.txt
        @a amber status
        assert_output_contains "new test.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_rm_not_existing_file() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        # action
        @a amber rm does-not-exist
        assert_output_contains "already deleted"
    "#;
    dsl_definition::run_dsl_script(script).await
}
