mod dsl_definition;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_deduplicate() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file test-1.txt "I am not a snowflake"
        @a write_file test-2.txt "I am not a snowflake"

        # action
        @a amber add

        # then
        @a assert_hardlinked test-1.txt test-2.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_nested_dirs_and_skip_deduplication() -> anyhow::Result<(), anyhow::Error>
{
    let script = r#"
        # Initialize repositories
        @a amber init a
        @b amber init b
        
        # Create nested directory structures with files in repository A
        @a write_file dir1/file1.txt "Content in dir1/file1.txt"
        @a write_file dir1/file2.txt "Content in dir1/file2.txt"
        @a write_file dir1/subdir/file3.txt "Content in dir1/subdir/file3.txt"
        @a write_file dir2/file4.txt "Content in dir2/file4.txt"
        @a write_file dir2/file5.txt "Identical content"
        @a write_file dir2/file6.txt "Identical content"
        
        # Add with skip-deduplication flag to prevent hardlinking of identical files
        @a amber add --skip-deduplication
        
        # Verify files are not hardlinked despite having identical content
        @a amber status
        
        # Verify files with identical content are NOT hardlinked due to skip-deduplication
        @a assert_not_hardlinked dir2/file5.txt dir2/file6.txt
        
        # Connect repositories
        @a amber remote add b local $ROOT/b
        
        # Test selective sync with complex path pattern (only dir1)
        @a amber push b dir1
        @b amber sync
        
        # Verify dir1 and its contents were synced
        @b assert_exists dir1/file1.txt "Content in dir1/file1.txt"
        @b assert_exists dir1/file2.txt "Content in dir1/file2.txt"
        @b assert_exists dir1/subdir/file3.txt "Content in dir1/subdir/file3.txt"
        
        # Verify dir2 was not synced
        @b assert_does_not_exist dir2/file4.txt
        
        # Now sync everything
        @a amber push b
        @b amber sync
        
        # Verify all files are now present
        @b assert_exists dir2/file4.txt "Content in dir2/file4.txt"
        @b assert_exists dir2/file5.txt "Identical content"
        @b assert_exists dir2/file6.txt "Identical content"
        
        # Test that normal add DOES deduplicate files
        @b write_file new_file1.txt "Duplicate content"
        @b write_file new_file2.txt "Duplicate content"
        @b amber add
        
        # Verify these new files ARE hardlinked (default behavior)
        @b assert_hardlinked new_file1.txt new_file2.txt
        
        # Verify after full sync, repositories are identical
        @a amber pull b
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}
