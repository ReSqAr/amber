use anyhow::anyhow;
use std::collections::HashMap;
use std::sync::Once;
use tempfile::tempdir;
use types::{CommandLine, RepoInstance, TestEnv};

mod amber;
mod asserts;
mod db;
mod fs;
mod parser;
mod ssh;
mod types;
mod writer;

static INIT: Once = Once::new();
fn init_logger() {
    INIT.call_once(|| {
        env_logger::init();
    });
}

/// Run the DSL script. This function creates a temporary $ROOT directory,
/// then processes each DSL line.
pub async fn run_dsl_script(script: &str) -> anyhow::Result<(), anyhow::Error> {
    init_logger();

    // Create a temporary root directory.
    let tmp_dir = tempdir()?;
    let root = tmp_dir.path().to_path_buf();
    let mut last_command_output: String = "".into();

    let mut env = TestEnv {
        root: root.clone(),
        repos: HashMap::new(),
    };

    let mut ssh_connection = HashMap::new();

    let mut line_number = 0usize;
    for line in script.lines() {
        line_number += 1;
        println!("[{:2}] {}", line_number, line.trim());
        if let Some(cmd) = parser::parse_line(line) {
            match cmd {
                CommandLine::AmberCommand { repo, sub_command } => {
                    let repo_instance = env.repos.entry(repo.clone()).or_insert_with(|| {
                        let repo_path = root.join(&repo);
                        std::fs::create_dir_all(&repo_path)
                            .expect("failed to create repository folder");
                        RepoInstance {
                            id: repo.clone(),
                            path: repo_path,
                        }
                    });
                    // Call run_cli_command, passing in the global root for $ROOT substitution.
                    last_command_output = amber::run_amber_cli_command(
                        &sub_command,
                        &repo_instance.path,
                        &root,
                        None,
                    )
                    .await?;
                }
                CommandLine::AmberCommandFailure {
                    repo,
                    sub_command,
                    expected_failure,
                } => {
                    let repo_instance = env.repos.entry(repo.clone()).or_insert_with(|| {
                        let repo_path = root.join(&repo);
                        std::fs::create_dir_all(&repo_path)
                            .expect("failed to create repository folder");
                        RepoInstance {
                            id: repo.clone(),
                            path: repo_path,
                        }
                    });
                    // Call run_cli_command, passing in the global root for $ROOT substitution.
                    last_command_output = amber::run_amber_cli_command(
                        &sub_command,
                        &repo_instance.path,
                        &root,
                        Some(expected_failure),
                    )
                    .await?;
                }
                CommandLine::RandomFile {
                    repo,
                    filename,
                    size,
                } => {
                    let repo_instance = env.repos.entry(repo.clone()).or_insert_with(|| {
                        let repo_path = root.join(&repo);
                        std::fs::create_dir_all(&repo_path)
                            .expect("failed to create repository folder");
                        RepoInstance {
                            id: repo.clone(),
                            path: repo_path,
                        }
                    });
                    fs::create_random_file(&repo_instance.path, &filename, size).await?;
                }
                CommandLine::WriteFile {
                    repo,
                    filename,
                    data,
                } => {
                    let repo_instance = env.repos.entry(repo.clone()).or_insert_with(|| {
                        let repo_path = root.join(&repo);
                        std::fs::create_dir_all(&repo_path)
                            .expect("failed to create repository folder");
                        RepoInstance {
                            id: repo.clone(),
                            path: repo_path,
                        }
                    });
                    fs::write_file(&repo_instance.path, &filename, &data).await?;
                }
                CommandLine::RemoveFile { repo, filename } => {
                    let repo_instance = env.repos.entry(repo.clone()).or_insert_with(|| {
                        let repo_path = root.join(&repo);
                        std::fs::create_dir_all(&repo_path)
                            .expect("failed to create repository folder");
                        RepoInstance {
                            id: repo.clone(),
                            path: repo_path,
                        }
                    });
                    fs::remove_file(&repo_instance.path, &filename).await?;
                }
                CommandLine::AssertExists {
                    repo,
                    filename,
                    content,
                } => {
                    let repo_instance = env.repos.get(&repo).ok_or_else(|| {
                        anyhow!("Repository {} not found for assert_exists command", repo)
                    })?;
                    asserts::assert_file_exists(&repo_instance.path, &filename, &content).await?;
                }
                CommandLine::AssertDoesNotExist { repo, filename } => {
                    let repo_instance = env.repos.get(&repo).ok_or_else(|| {
                        anyhow!(
                            "repository {} not found for assert_does_not_exist command",
                            repo
                        )
                    })?;
                    asserts::assert_file_does_not_exist(&repo_instance.path, &filename).await?;
                }
                CommandLine::AssertEqual {
                    left_repo,
                    right_repo,
                } => {
                    let left = env.repos.get(&left_repo).ok_or_else(|| {
                        anyhow!("repository {} not found for assert_equal", left_repo)
                    })?;
                    let right = env.repos.get(&right_repo).ok_or_else(|| {
                        anyhow!("repository {} not found for assert_equal", right_repo)
                    })?;
                    asserts::assert_directories_equal(&left.path, &right.path)
                        .await
                        .map_err(|e| anyhow!("assert_equal failed: {}", e))?;
                }
                CommandLine::AssertHardlinked {
                    repo,
                    filename1,
                    filename2,
                } => {
                    let repo_instance = env.repos.get(&repo).ok_or_else(|| {
                        anyhow!(
                            "repository {} not found for assert_hardlinked command",
                            repo
                        )
                    })?;
                    asserts::assert_files_hardlinked(&repo_instance.path, &filename1, &filename2)
                        .await?;
                }
                CommandLine::AssertNotHardlinked {
                    repo,
                    filename1,
                    filename2,
                } => {
                    let repo_instance = env.repos.get(&repo).ok_or_else(|| {
                        anyhow!(
                            "repository {} not found for assert_not_hardlinked command",
                            repo
                        )
                    })?;
                    asserts::assert_files_not_hardlinked(
                        &repo_instance.path,
                        &filename1,
                        &filename2,
                    )
                    .await?;
                }
                CommandLine::AssertOutputContains { expected } => {
                    if !last_command_output.contains(&expected) {
                        return Err(anyhow!(
                            "assert_output_contains failed: output did not contain '{}'",
                            expected
                        ));
                    }
                }
                CommandLine::StartSsh {
                    repo,
                    port,
                    password,
                } => {
                    let repo_instance = env.repos.get(&repo).ok_or_else(|| {
                        anyhow!("Repository {} not found for start_ssh command", repo)
                    })?;

                    let shutdown =
                        ssh::start_ssh_server(&repo_instance.path, ".amb".into(), port, password)
                            .await?;
                    if let Some(s) = ssh_connection.insert(repo, shutdown) {
                        s.await
                    }
                }
                CommandLine::EndSsh { repo } => {
                    if let Some(s) = ssh_connection.remove(&repo) {
                        s.await
                    }
                }
                CommandLine::Sql { repo, sql } => {
                    let repo_instance = env.repos.get(&repo).ok_or_else(|| {
                        anyhow!("Repository {} not found for start_ssh command", repo)
                    })?;
                    db::run_sql(&repo_instance.path, sql).await?;
                }
            }
        }
    }
    Ok(())
}
