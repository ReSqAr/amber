mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_missing_files() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        # Initialize repositories
        @a amber init a
        @b amber init b

        # Create nested directory structures with files in repository a
        @a write_file README.MD "Content in README.md"
        @a write_file dir1/file1.txt "Content in dir1/file1.txt"
        @a write_file dir1/file2.txt "Content in dir1/file2.txt"
        @a write_file dir1/subdir/file3.txt "Content in dir1/subdir/file3.txt"
        @a write_file dir2/file4.txt "Content in dir2/file4.txt"
        @a write_file dir2/file5.txt "Identical content"
        @a write_file dir2/file6.txt "Identical content"

        # Add files
        @a amber add

        @a amber status

        # Connect repositories
        @a amber remote add b local $ROOT/b
        @a amber sync b

        # Missing
        @b amber missing --summary
        assert_output_contains "detected 7 missing files out of 7 files in"
        @b amber missing --summary 0
        assert_output_contains "detected 7 missing files out of 7 files in"
        @b amber missing --summary 1
        assert_output_contains "dir1/ 3 missing (3 total)"
        assert_output_contains "dir2/ 3 missing (3 total)"

        # Selective sync
        @a amber push b dir1
        @b amber sync

        # Verify dir1 and its contents were synced
        @b assert_exists dir1/file1.txt "Content in dir1/file1.txt"
        @b assert_exists dir1/file2.txt "Content in dir1/file2.txt"
        @b assert_exists dir1/subdir/file3.txt "Content in dir1/subdir/file3.txt"

        # Missing
        @b amber missing --summary
        assert_output_contains "detected 4 missing files out of 7 files in"
        @b amber missing --summary 0
        assert_output_contains "detected 4 missing files out of 7 files in"
        @b amber missing --summary 1
        assert_output_contains "dir2/ 3 missing (3 total)"

        # Verify dir2 was not synced
        @b assert_does_not_exist dir2/file4.txt

        # Now sync everything
        @a amber push b
        @b amber sync

        # Verify all files are now present
        @b assert_exists dir2/file4.txt "Content in dir2/file4.txt"
        @b assert_exists dir2/file5.txt "Identical content"
        @b assert_exists dir2/file6.txt "Identical content"

        # Missing
        @b amber missing --summary
        assert_output_contains "detected 0 missing files out of 7 files in"
        @b amber missing --summary 0
        assert_output_contains "detected 0 missing files out of 7 files in"
        @b amber missing --summary 1
        assert_output_contains "detected 0 missing files out of 7 files in"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_missing_with_files() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        # Initialize repositories
        @a amber init a

        # Missing
        @a amber missing -s
        assert_output_contains "no files detected in"
        @a amber missing -s 0
        assert_output_contains "no files detected in"
        @a amber missing -s 1
        assert_output_contains "no files detected in"
    "#;
    dsl_definition::run_dsl_script(script).await
}
