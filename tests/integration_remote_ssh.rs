mod dsl_definition;

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_ssh_connection_list() -> Result<(), anyhow::Error> {
    let script = r#"
        @a amber init a
        @a start_ssh 45670 hunter2

        @b amber init b

        @b amber remote add a-ssh ssh "user:hunter2@localhost:45670/"
        
        @b amber remote list
        assert_output_contains "a-ssh"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_ssh_connection_sync() -> Result<(), anyhow::Error> {
    let script = r#"
        # Set up repositories
        @a amber init a
        @a write_file test1.txt "Common content!"
        @a write_file test2.txt "Common content!"
        @a amber add

        @b amber init b
        @b write_file test1.txt "Common content!"
        @b amber add

        # Start SSH server for repository A
        @a start_ssh 45671 hunter2

        # Add remote via SSH connection
        @b amber remote add a-ssh ssh "user:hunter2@localhost:45671/"

        # Pull from SSH remote
        @b amber sync a-ssh

        # Verify the file was transferred
        @b assert_exists test2.txt "Common content!"
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_ssh_connection_pull() -> Result<(), anyhow::Error> {
    let script = r#"
        # Set up repositories
        @a amber init a
        @a write_file test.txt "Hello from repository A!"
        @a amber add

        @b amber init b

        # Start SSH server for repository A
        @a start_ssh 45672 hunter2

        # Add remote via SSH connection from B to A
        @b amber remote add a-ssh ssh "user:hunter2@localhost:45672/"

        # Pull from SSH remote A
        @b amber pull a-ssh

        # Verify the file was transferred
        @b assert_exists test.txt "Hello from repository A!"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_ssh_connection_push() -> Result<(), anyhow::Error> {
    let script = r#"
        # Set up repositories
        @a amber init a
        @a write_file test.txt "Hello from repository A!"
        @a amber add

        @b amber init b

        # Start SSH server for repository B
        @b start_ssh 45673 hunter2

        # Add remote via SSH connection from A to B
        @a amber remote add b-ssh ssh "user:hunter2@localhost:45673/"

        # Push from A to SSH remote B
        @a amber push b-ssh

        @b end_ssh
        @b amber sync

        # Verify the file was transferred
        @b assert_exists test.txt "Hello from repository A!"
    "#;
    dsl_definition::run_dsl_script(script).await
}
