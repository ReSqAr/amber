mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_mv() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
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
async fn integration_test_mv_detect_changed_file() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        # action
        @a remove_file test.txt
        @a write_file test.txt "overwritten"
        @a amber mv test.txt test.moved

        # then
        @a amber status
        assert_output_contains "altered test.moved"
        @a assert_exists test.moved "overwritten"
        @a assert_does_not_exist test.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_mv_dir_to_dir() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file dir/x.txt "X"
        @a write_file dir/y.txt "Y"
        @a amber add

        @b amber init b
        @a amber remote add b local $ROOT/b

        @a amber push b
        @b amber sync
        assert_equal a b

        # action
        @a amber mv dir dir_new
        @a amber sync b
        @b amber sync

        # assert
        @a assert_exists dir_new/x.txt "X"
        @a assert_exists dir_new/y.txt "Y"
        @a assert_does_not_exist dir/x.txt
        @a assert_does_not_exist dir/y.txt

        @b assert_exists dir_new/x.txt "X"
        @b assert_exists dir_new/y.txt "Y"
        @b assert_does_not_exist dir/x.txt
        @b assert_does_not_exist dir/y.txt

        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_mv_not_yet_pushed_file() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file mv-before-push.txt "b can move file without knowing its contents"
        @a amber add

        @b amber init b
        @a amber remote add b local $ROOT/b
        @a amber sync b
        @b amber sync
        @b assert_does_not_exist mv-before-push.txt

        # action  1
        @b amber mv mv-before-push.txt moved-db-only.txt
        @a amber sync b
        @b amber sync

        # assert 1
        @a assert_does_not_exist mv-before-push.txt
        @b assert_does_not_exist mv-before-push.txt
        @b assert_does_not_exist moved-db-only.txt
        @a assert_exists moved-db-only.txt "b can move file without knowing its contents"

        # action 2
        @a amber push b
        @b amber sync

        # assert 2
        @b assert_exists moved-db-only.txt "b can move file without knowing its contents"
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_mv_not_yet_pushed_dir() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file mv-before-push/x.txt "X"
        @a write_file mv-before-push/y.txt "Y"
        @a amber add

        @b amber init b
        @a amber remote add b local $ROOT/b
        @a amber sync b
        @b amber sync
        @b assert_does_not_exist mv-before-push/x.txt
        @b assert_does_not_exist mv-before-push/y.txt

        # action  1
        @b amber mv mv-before-push moved-db-only
        @a amber sync b
        @b amber sync

        # assert 1
        @a assert_does_not_exist mv-before-push/x.txt
        @a assert_does_not_exist mv-before-push/y.txt
        @b assert_does_not_exist mv-before-push/x.txt
        @b assert_does_not_exist mv-before-push/y.txt
        @b assert_does_not_exist moved-db-only/x.txt
        @b assert_does_not_exist moved-db-only/y.txt
        @a assert_exists moved-db-only/x.txt "X"
        @a assert_exists moved-db-only/y.txt "Y"

        # action 2
        @a amber push b
        @b amber sync

        # assert 2
        @b assert_exists moved-db-only/x.txt "X"
        @b assert_exists moved-db-only/y.txt "Y"
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_mv_destination_file_exists() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file a.txt "A"
        @a write_file b.txt "B"
        @a amber add

        # action
        @a expect "destination a.txt does already exist" amber mv a.txt b.txt

        # assert
        @a assert_exists a.txt "A"
        @a assert_exists b.txt "B"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_mv_source_equals_destination() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file a.txt "A"
        @a amber add

        # action
        @a expect "destination a.txt does already exist" amber mv a.txt a.txt

        # assert
        @a assert_exists a.txt "A"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_mv_recursive_dir_into_subdir_allowed() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file a/x.txt "X"
        @a amber add

        @b amber init b
        @a amber remote add b local $ROOT/b

        @a amber push b
        @b amber sync
        assert_equal a b

        # action
        @a amber mv a a/sub
        @a amber sync b
        @b amber sync

        # assert
        @a assert_exists a/sub/x.txt "X"
        @a assert_does_not_exist a/x.txt
        @b assert_exists a/sub/x.txt "X"
        @b assert_does_not_exist a/x.txt
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_mv_trailing_slash_dir_hint() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file d/f.txt "F"
        @a amber add

        @b amber init b
        @a amber remote add b local $ROOT/b
        @a amber push b
        @b amber sync
        assert_equal a b

        # action
        @a amber mv d/ d2
        @a amber sync b
        @b amber sync

        # assert
        @a assert_exists d2/f.txt "F"
        @a assert_does_not_exist d/f.txt
        @b assert_exists d2/f.txt "F"
        @b assert_does_not_exist d/f.txt
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_mv_dir() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file dir/a.txt "A"
        @a amber add

        @b amber init b
        @a amber remote add b local $ROOT/b
        @a amber push b
        @b amber sync
        assert_equal a b

        # action
        @a amber mv dir out
        @a amber sync b
        @b amber sync

        @a amber status
        @b amber status

        # assert
        @a assert_exists out/a.txt "A"
        @a assert_does_not_exist dir/a.txt
        @b assert_exists out/a.txt "A"
        @b assert_does_not_exist dir/a.txt
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_mv_file_into_dir_forbidden() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file f.txt "F"
        @a amber add

        # action
        @a expect "destination d/ is a folder - expected it to be a file" amber mv f.txt d/

        # assert
        @a assert_exists f.txt "F"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_mv_no_files_to_move() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file x.txt "X"
        @a amber add

        # action
        @a expect "move encountered errors" amber mv y.txt z.txt
        assert_output_contains "no files match selector"

        # assert
        @a assert_exists x.txt "X"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_mv_protect_tracked_files() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file x.txt "X"
        @a write_file y.txt "Y"
        @a amber add

        @b amber init b
        @a amber remote add b local $ROOT/b
        @a amber sync b
        @b amber sync
        @b assert_does_not_exist x.txt
        @b assert_does_not_exist y.txt

        # action  1
        @b expect "move encountered errors" amber mv x.txt y.txt
        assert_output_contains "destination already exists y.txt"
        @a amber sync b
        @b amber sync

        # assert 1
        @b assert_does_not_exist x.txt
        @b assert_does_not_exist y.txt
        @a assert_exists x.txt "X"
        @a assert_exists y.txt "Y"

        # action 2
        @a amber push b
        @b amber sync

        # assert 2
        @b assert_exists x.txt "X"
        @b assert_exists y.txt "Y"
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}
#[tokio::test(flavor = "multi_thread")]
async fn integration_mv_protect_existing_files() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file dir/x.txt "X"
        @a write_file dir/y.txt "Y"
        @a amber add

        # action
        @a write_file dir_new/x.txt "untracked file does not get overwritten"
        @a expect "move encountered errors" amber mv dir dir_new
        assert_output_contains "destination dir_new/x.txt already exists dir/x.txt"

        # assert
        @a assert_exists dir_new/x.txt "untracked file does not get overwritten"
        @a assert_exists dir/x.txt "X"
        @a assert_exists dir_new/y.txt "Y"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_rm() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        # action
        @a amber rm --hard test.txt
        assert_output_contains "removed test.txt"

        # then
        @a assert_does_not_exist test.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_rm_soft() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        # action
        @a amber rm test.txt
        assert_output_contains "removed [soft] test.txt"

        # then
        @a assert_exists test.txt
        @a amber status
        assert_output_contains "new test.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_rm_not_existing_file() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        # action
        @a expect "remove encountered errors" amber rm --hard does-not-exist
        assert_output_contains "no files match selector does-not-exist"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_rm_dir_prefix() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file d/a.txt "A"
        @a write_file d/b.txt "B"
        @a amber add



        # action
        @a amber rm --hard d

        # then
        @a assert_does_not_exist d/a.txt
        @a assert_does_not_exist d/b.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_rm_db_only_file() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file ghost.txt "G"
        @a amber add

        @a remove_file ghost.txt
        @a amber status

        # action
        @a amber rm --hard ghost.txt
        assert_output_contains "removed (not materialised) ghost.txt"

        # then
        @a assert_does_not_exist ghost.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_rm_trailing_slash_dir_hint() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file dir/x.txt "X"
        @a write_file dir/y.txt "Y"
        @a amber add

        # action
        @a amber rm --hard dir/
        assert_output_contains "removed 2 files (2 on disk)"

        # then
        @a assert_does_not_exist dir/x.txt
        @a assert_does_not_exist dir/y.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_rm_trailing_slash_dir_hint_in_db() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file dir/x.txt "X"
        @a write_file dir/y.txt "Y"
        @a amber add

        @a remove_file dir/x.txt
        @a remove_file dir/y.txt

        # action
        @a amber rm --hard dir/
        assert_output_contains "removed 2 files (0 on disk)"

        # then
        @a assert_does_not_exist dir/x.txt
        @a assert_does_not_exist dir/y.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_rm_trailing_slash_dir_hint_in_db_conflict_file()
-> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file actual_file "F"
        @a amber add

        # action
        @a expect "remove encountered errors" amber rm --hard actual_file/
        assert_output_contains "no files match selector actual_file"

        # then
        @a assert_exists actual_file "F"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_rm_multi_input_partial_success() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file keep.txt "K"
        @a write_file del.txt "D"
        @a amber add

        # action
        @a expect "remove encountered errors" amber rm --hard del.txt does-not-exist
        assert_output_contains "no files match selector does-not-exist"

        # then
        @a assert_exists del.txt
        @a assert_exists keep.txt "K"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_rm_overlapping_inputs_dir_and_child() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file d/x.txt "X"
        @a amber add

        # action
        @a amber rm --hard d d/x.txt

        # then
        @a assert_does_not_exist d/x.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_rm_hard_keeps_modified_file() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file keep.txt "original"
        @a amber add

        # modify the materialised file after it has been tracked
        @a remove_file keep.txt
        @a write_file keep.txt "changed"

        # removal should refuse to delete because the timestamp differs
        @a amber rm --hard keep.txt
        assert_output_contains "kept modified file keep.txt"
        assert_output_contains "removed 1 files (0 on disk)"

        # the file stays on disk and becomes a new untracked file
        @a assert_exists keep.txt "changed"
        @a amber status
        assert_output_contains "new keep.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_rm_hard_keeps_modified_file_within_dir() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file dir/keep.txt "K"
        @a write_file dir/remove.txt "R"
        @a amber add

        # change only one of the files before removing the directory
        @a remove_file dir/keep.txt
        @a write_file dir/keep.txt "K2"

        @a amber rm --hard dir
        assert_output_contains "kept modified file dir/keep.txt"
        assert_output_contains "removed dir/remove.txt"
        assert_output_contains "removed 2 files (1 on disk)"

        @a assert_exists dir/keep.txt "K2"
        @a assert_does_not_exist dir/remove.txt
        @a amber status
        assert_output_contains "new dir/keep.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_rm_hard_keeps_modified_file_after_status() -> anyhow::Result<(), anyhow::Error>
{
    let script = r#"
        @a amber init a
        @a write_file keep.txt "original"
        @a amber add

        # modify the materialised file after it has been tracked
        @a remove_file keep.txt
        @a write_file keep.txt "changed"
        @a amber status

        # removal should refuse to delete because the timestamp differs
        @a amber rm --hard keep.txt
        assert_output_contains "kept modified file keep.txt"
        assert_output_contains "removed 1 files (0 on disk)"

        # the file stays on disk and becomes a new untracked file
        @a assert_exists keep.txt "changed"
        @a amber status
        assert_output_contains "new keep.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_rm_hard_keeps_modified_file_within_dir_after_status()
-> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file dir/keep.txt "K"
        @a write_file dir/remove.txt "R"
        @a amber add

        # change only one of the files before removing the directory
        @a remove_file dir/keep.txt
        @a write_file dir/keep.txt "K2"
        @a amber status

        @a amber rm --hard dir
        assert_output_contains "kept modified file dir/keep.txt"
        assert_output_contains "removed dir/remove.txt"
        assert_output_contains "removed 2 files (1 on disk)"

        @a assert_exists dir/keep.txt "K2"
        @a assert_does_not_exist dir/remove.txt
        @a amber status
        assert_output_contains "new dir/keep.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_rm_soft_dir() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file d/a.txt "A"
        @a write_file d/b.txt "B"
        @a amber add

        @a amber rm d
        assert_output_contains "removed [soft] d/a.txt"
        assert_output_contains "removed [soft] d/b.txt"

        # files still on disk but untracked now
        @a assert_exists d/a.txt "A"
        @a assert_exists d/b.txt "B"
        @a amber status
        assert_output_contains "new d/a.txt"
        assert_output_contains "new d/b.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_rm_push_sync_propagation() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file f.txt "F"
        @a amber add

        @b amber init b
        @a amber remote add b local $ROOT/b

        @a amber push b
        @b amber sync
        assert_equal a b

        # remove on A, propagate
        @a amber rm --hard f.txt
        @a amber push b
        @b amber sync

        @b assert_does_not_exist f.txt
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_mv_changed_file_and_then_delete() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file test.txt "Hello world!"
        @a amber add

        # action 1
        @a remove_file test.txt
        @a write_file test.txt "overwritten"
        @a amber mv test.txt test.moved
        @a amber rm --hard test.moved

        # then
        @a amber status
        assert_output_contains "new test.moved"
        @a assert_exists test.moved "overwritten"
        @a assert_does_not_exist test.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}
