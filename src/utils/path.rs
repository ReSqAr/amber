use std::path::{Display, Path, PathBuf};

#[derive(Clone, Debug)]
pub struct RepoPath {
    relative: PathBuf,
    absolute: PathBuf,
}

impl RepoPath {
    pub fn from_root(root: PathBuf) -> Self {
        Self {
            relative: "".into(),
            absolute: root,
        }
    }

    pub fn rel(&self) -> &PathBuf {
        &self.relative
    }

    pub fn abs(&self) -> &PathBuf {
        &self.absolute
    }

    pub fn join<P: AsRef<Path>>(&self, path: P) -> Self {
        Self {
            relative: self.relative.join(&path),
            absolute: self.absolute.join(path),
        }
    }

    pub(crate) fn display(&self) -> Display<'_> {
        self.absolute.display()
    }
}

impl AsRef<Path> for RepoPath {
    fn as_ref(&self) -> &Path {
        self.absolute.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::path::RepoPath;
    use std::path::PathBuf;

    #[test]
    fn test_repo_path_join() {
        let root = PathBuf::from("/my/repo");
        let repo_path = RepoPath::from_root(root.clone());
        let sub = repo_path.join("subdir/file.txt");
        assert_eq!(sub.rel().to_string_lossy(), "subdir/file.txt");
        assert_eq!(sub.abs().to_string_lossy(), "/my/repo/subdir/file.txt");
    }
}
