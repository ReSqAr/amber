mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_nested_dirs() -> anyhow::Result<(), anyhow::Error> {
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
        
        # Add files
        @a amber add
        
        # Verify files are not hardlinked despite having identical content
        @a amber status

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

        # Verify after full sync, repositories are identical
        @a amber pull b
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}
