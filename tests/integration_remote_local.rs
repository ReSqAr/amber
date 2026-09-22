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
        @a amber remote add b local $ROOT/b
        @a write_file test.txt "Hello A world!"
        @a amber add
        @b write_file test.txt "Hello B world!"
        @b amber add

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
        @a amber remote add b local $ROOT/b
        @a write_file test.txt "Hello world - I am A!"
        @a amber add
        @b write_file test.txt "Hello world - I am B!"
        @b amber add

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
        @a amber remote add b local $ROOT/b
        @a write_file test-a.txt "Hello A world!"
        @a amber add
        @b write_file test-b.txt "Hello B world!"
        @b amber add

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

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_remote_add_refuses_unrelated_repositories() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file a.txt "only in a"
        @a amber add

        @b amber init b
        @b write_file b.txt "only in b"
        @b amber add

        # action
        @a expect "shares no history with this repository" amber remote add b local $ROOT/b

        # then
        @a expect "connection b not found" amber sync b
        @a assert_does_not_exist b.txt
        @b assert_does_not_exist a.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_remote_add_refuses_emptied_unrelated_repository()
-> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file a.txt "only in a"
        @a amber add

        @b amber init b
        @b write_file b.txt "once in b"
        @b amber add
        @b amber rm --hard b.txt

        # action - b holds no files anymore but its history is not empty
        @a expect "shares no history with this repository" amber remote add b local $ROOT/b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_remote_add_accepts_virgin_target() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file a.txt "from a"
        @a amber add

        @b amber init b

        # action
        @a amber remote add b local $ROOT/b
        @a amber push b
        @b amber sync

        # then
        @b assert_exists a.txt "from a"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_remote_add_accepts_virgin_local() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        @b amber init b
        @b write_file b.txt "from b"
        @b amber add

        # action
        @a amber remote add b local $ROOT/b
        @a amber pull b

        # then
        @a assert_exists b.txt "from b"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_remote_add_accepts_repositories_related_via_a_third()
-> Result<(), anyhow::Error> {
    let script = r#"
        # when - a and c never met, but both synced with b
        @a amber init a
        @a write_file a.txt "from a"
        @a amber add

        @b amber init b
        @a amber remote add b local $ROOT/b
        @a amber push b

        @c amber init c
        @c amber remote add b local $ROOT/b
        @c amber pull b
        @c write_file c.txt "from c"
        @c amber add

        # action
        @a amber remote add c local $ROOT/c
        @a amber pull c

        # then
        @a assert_exists c.txt "from c"
    "#;
    dsl_definition::run_dsl_script(script).await
}
