use serial_test::serial;
mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_new_file_state() -> Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file new.txt "I am new"
        @a amber status
        assert_output_contains "new new.txt"
        @a assert_exists new.txt "I am new"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_altered_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Original content"
        @a amber add
        # Simulate user alteration by overwriting the file with wrong content.
        @a remove_file test.txt
        @a write_file test.txt "User altered content"

        @a amber sync

        # action
        @a amber status

        # then: we do not overwrite the on-disk file but do alert the user
        assert_output_contains "altered test.txt"
        @a assert_exists test.txt "User altered content"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_outdated_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello A world!"
        @a amber add

        @b amber init b
        @b write_file test.txt "Hello B world!"
        @b amber add

        @a amber remote add b local $ROOT/b
        @a amber sync b

        # action
        @a amber status
        # then
        assert_output_contains "outdated test.txt"

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
#[serial]
async fn integration_test_missing_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        @b amber init b
        @b write_file test.txt "Hello B world!"
        @b amber add

        @a amber remote add b local $ROOT/b
        @a amber sync b

        # action
        @a amber status
        # then
        assert_output_contains "missing test.txt"

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
#[serial]
async fn integration_test_delete_synced_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test-a.txt "Hello A world!"
        @a amber add

        @b amber init b
        @b write_file test-b.txt "Hello B world!"
        @b amber add

        @a amber remote add b local $ROOT/b
        @a amber sync b

        # action 1
        @a amber status
        # then
        assert_output_contains "missing test-b.txt"

        # action 2
        @b amber status
        # then
        assert_output_contains "missing test-a.txt"

        # when
        @a amber pull b
        @a amber status
        @b amber status

        # action 3
        @a amber remove test-a.txt test-b.txt
        @a amber sync b
        @b amber sync

        # then
        assert_equal a b
        @a assert_does_not_exist test-a.txt
        @a assert_does_not_exist test-b.txt
        @b assert_does_not_exist test-a.txt
        @b assert_does_not_exist test-b.txt

        @a amber status
        assert_output_contains "no files detected"
        @b amber status
        assert_output_contains "no files detected"

        @a amber missing
        assert_output_contains "no files missing"
        @b amber missing
        assert_output_contains "no files missing"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_two_repo_status_missing() -> Result<(), anyhow::Error> {
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
        @a amber status
        assert_output_contains "missing test-b.txt"
        @b amber status
        assert_output_contains "missing test-a.txt"

        # action 2
        @a amber pull b

        # then
        @a amber status
        assert_output_contains "detected 2 materialised files"
        @b amber status
        assert_output_contains "missing test-a.txt"

        # action 3
        @a amber push b
        @b amber sync

        # then
        @a amber status
        assert_output_contains "detected 2 materialised files"
        @b amber status
        assert_output_contains "detected 2 materialised files"
    "#;
    dsl_definition::run_dsl_script(script).await
}
