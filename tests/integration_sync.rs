mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_two_repo_sync_pull_push() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a random_file test-a.txt 100
        @a amber add

        @b amber init b
        @a amber remote add b local $ROOT/b
        @b write_file test-b.txt "Hello world!"
        @b amber add

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
        @a amber remote add b local $ROOT/b
        @b write_file b1.txt "b1"
        @b amber add

        @c amber init c
        @a amber remote add c local $ROOT/c
        @c write_file c1.txt "c1"
        @c write_file c2.txt "c2"
        @c write_file c3.txt "c3"
        @c amber add

        @a amber sync b

        # a's log now grows past b's via the third repository
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

/// A file removed in another repository is deleted here on sync - but only if
/// it still holds what amber put there. An edit amber has not recorded yet is
/// the user's own file and must survive.
#[tokio::test(flavor = "multi_thread")]
async fn integration_test_sync_keeps_unrecorded_edit_of_removed_file() -> Result<(), anyhow::Error>
{
    let script = r#"
        # when
        @a amber init a
        @a write_file edited.txt "original"
        @a write_file untouched.txt "untouched"
        @a amber add

        @b amber init b
        @b amber remote add a local $ROOT/a
        @b amber pull a
        # replace the file (writing through the hard link would change the blob)
        @b remove_file edited.txt
        @b write_file edited.txt "my edit"

        @a amber remove edited.txt untouched.txt

        # action
        @b amber sync a

        # then
        @b assert_exists edited.txt "my edit"
        @b assert_does_not_exist untouched.txt
        @b amber status
        assert_output_contains "new edited.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}

/// The same for a file renamed in another repository: the new name is
/// materialised, and the edited copy under the old name is kept.
#[tokio::test(flavor = "multi_thread")]
async fn integration_test_sync_keeps_unrecorded_edit_of_moved_file() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file old.txt "original"
        @a amber add

        @b amber init b
        @b amber remote add a local $ROOT/a
        @b amber pull a
        # replace the file (writing through the hard link would change the blob)
        @b remove_file old.txt
        @b write_file old.txt "my edit"

        @a amber mv old.txt new.txt

        # action
        @b amber sync a

        # then
        @b assert_exists old.txt "my edit"
        @b assert_exists new.txt "original"
    "#;
    dsl_definition::run_dsl_script(script).await
}

/// A path the repository wants but that is already taken on disk - on a
/// filesystem that ignores case or Unicode normalisation, `Photo.jpg` finds
/// `photo.jpg` - is skipped and reported instead of replacing what is there.
/// A symlink takes the path here, since CI has no such filesystem.
#[tokio::test(flavor = "multi_thread")]
async fn integration_test_sync_skips_a_path_taken_on_disk() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file x.txt "from a"
        @a write_file y.txt "also from a"
        @a amber add

        @b amber init b
        @b write_file other.txt "kept"
        @b symlink other.txt x.txt
        @a amber remote add b local $ROOT/b

        # action
        @a amber push b
        @b amber sync
        assert_output_contains "skipped 1 files whose path another file already takes"

        # then
        @b assert_exists x.txt "kept"
        @b assert_exists other.txt "kept"
        @b assert_exists y.txt "also from a"
    "#;
    dsl_definition::run_dsl_script(script).await
}
