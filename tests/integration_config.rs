use serial_test::serial;

mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn integration_test_config_set_name() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a

        # action
        @a amber config set-name b
    "#;
    dsl_definition::run_dsl_script(script).await
}
