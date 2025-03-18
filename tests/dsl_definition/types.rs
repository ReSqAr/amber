use std::collections::HashMap;
use std::path::PathBuf;

/// Refined DSL command enum.
#[derive(Debug)]
pub(crate) enum CommandLine {
    AmberCommand {
        repo: String,
        sub_command: Vec<String>,
    },
    RandomFile {
        repo: String,
        filename: String,
        size: usize,
    },
    WriteFile {
        repo: String,
        filename: String,
        data: String,
    },
    RemoveFile {
        repo: String,
        filename: String,
    },
    AssertExists {
        repo: String,
        filename: String,
        content: Option<String>,
    },
    AssertDoesNotExist {
        repo: String,
        filename: String,
    },
    AssertEqual {
        left_repo: String,
        right_repo: String,
    },
    AssertHardlinked {
        repo: String,
        filename1: String,
        filename2: String,
    },
    AssertNotHardlinked {
        repo: String,
        filename1: String,
        filename2: String,
    },
    AssertOutputContains {
        expected: String,
    },
    StartSsh {
        repo: String,
        port: u16,
        password: String,
    },
    EndSsh {
        repo: String,
    },
    Sql {
        repo: String,
        sql: String,
    },
}

/// A repository instance in our DSL environment.
#[derive(Debug)]
pub struct RepoInstance {
    #[allow(dead_code)]
    pub(crate) id: String,
    pub(crate) path: PathBuf,
}

/// The test environment holds a temporary $ROOT directory and a map of repository instances.
#[derive(Debug)]
pub struct TestEnv {
    #[allow(dead_code)]
    pub(crate) root: PathBuf,
    pub(crate) repos: HashMap<String, RepoInstance>,
}
