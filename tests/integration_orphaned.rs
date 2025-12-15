mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_orphaned_blobs() -> anyhow::Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a write_file orphaned.txt "content"
        @a amber add

        @a amber orphaned
        assert_output_contains "0 orphaned blobs"

        # remove the file from tracking
        @a amber rm orphaned.txt
        @a amber orphaned
        assert_output_contains "3fba5250be9ac259c56e7250c526bc83bacb4be825f2799d3d59e5b4878dd74e orphaned.txt"
        assert_output_contains "1 orphaned blobs"
    "#;

    dsl_definition::run_dsl_script(script).await
}
