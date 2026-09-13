mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
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

/// The resume offsets used by `sync_repositories` are per-repository log offsets:
/// the download side must resume from how far we have consumed the *remote* log.
///
/// With only two repositories a complete bidirectional sync leaves both logs at the
/// same length, which hides a mix-up of the two offsets. A third repository breaks
/// that symmetry: `a` grows its log via `c`, so `a`'s own offset runs ahead of `b`'s
/// and resuming from it would skip everything `b` has to offer.
#[tokio::test(flavor = "multi_thread")]
async fn integration_test_three_repo_sync_resumes_per_repository() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file a1.txt "a1"
        @a amber add

        @b amber init b
        @b write_file b1.txt "b1"
        @b amber add

        @c amber init c
        @c write_file c1.txt "c1"
        @c write_file c2.txt "c2"
        @c write_file c3.txt "c3"
        @c amber add

        @a amber remote add b local $ROOT/b
        @a amber sync b

        # a's log now grows past b's via the third repository
        @a amber remote add c local $ROOT/c
        @a amber sync c

        # b gains a file after a last heard from it
        @b write_file b2.txt "b2"
        @b amber add

        # action
        @a amber sync b

        # then: a knows about every file of both peers
        @a amber status
        assert_output_contains "b1.txt"
        assert_output_contains "b2.txt"
        assert_output_contains "c1.txt"
        assert_output_contains "c2.txt"
        assert_output_contains "c3.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}
