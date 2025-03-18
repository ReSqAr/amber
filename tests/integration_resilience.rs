use serial_test::serial;

mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_resilience() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        # action
        @a sql 'DELETE FROM current_repository'
    "#;
    dsl_definition::run_dsl_script(script).await
}
