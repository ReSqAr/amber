mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_two_repo_push_path_selector() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file to-be-copied.txt "content to be transfered" 
        @a write_file not-transfered.txt "content only in a" 
        @a amber add

        @b amber init b

        @a amber remote add b local $ROOT/b

        # action
        @a amber push b to-be-copied.txt
        @b amber sync

        # then
        @b assert_exists to-be-copied.txt "content to be transfered"
        @b assert_does_not_exist not-transfered.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_two_repo_pull_path_selector() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        @b amber init b
        @b write_file to-be-copied.txt "content to be transfered" 
        @b write_file not-transfered.txt "content only in a" 
        @b amber add

        @a amber remote add b local $ROOT/b

        # action
        @a amber pull b to-be-copied.txt

        # then
        @a assert_exists to-be-copied.txt "content to be transfered"
        @a assert_does_not_exist not-transfered.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_two_repo_sync_same_filename_pull_push() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @b amber init b
        @a write_file test.txt "Hello A world!"
        @a amber add
        @b write_file test.txt "Hello B world!"
        @b amber add
        @a amber remote add b local $ROOT/b

        # action
        @a amber pull b
        @a amber push b
        @b amber sync

        # then
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_two_repo_sync_same_filename_push_pull() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @b amber init b
        @a write_file test.txt "Hello world - I am A!"
        @a amber add
        @b write_file test.txt "Hello world - I am B!"
        @b amber add
        @a amber remote add b local $ROOT/b

        # action
        @a amber push b
        @a amber pull b
        @b amber sync

        # then
        @a assert_exists test.txt "Hello world - I am B!"
        @b assert_exists test.txt "Hello world - I am B!"
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_two_repo_missing() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @b amber init b
        @a write_file test-a.txt "Hello A world!"
        @a amber add
        @b write_file test-b.txt "Hello B world!"
        @b amber add
        @a amber remote add b local $ROOT/b

        # action 1
        @a amber sync b
        
        # then
        @a amber missing
        assert_output_contains "missing test-b.txt (exists in: b)"
        @b amber missing
        assert_output_contains "missing test-a.txt (exists in: a)"

        # action 2
        @a amber pull b

        # then
        @a amber missing
        assert_output_contains "no files missing"
        @b amber missing
        assert_output_contains "missing test-a.txt (exists in: a)"

        # action 3
        @a amber push b

        # then
        @a amber missing
        assert_output_contains "no files missing"
        @b amber missing
        assert_output_contains "no files missing"
    "#;
    dsl_definition::run_dsl_script(script).await
}
