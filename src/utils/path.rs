use std::path::{Display, Path, PathBuf};

#[derive(Clone, Debug)]
pub struct RepoPath {
    relative: PathBuf,
    absolute: PathBuf,
}

impl RepoPath {
    pub fn from_root(root: PathBuf) -> Self {
        Self { relative: ".".into(), absolute: root }
    }

    pub fn rel(&self) -> PathBuf {
        self.relative.clone()
    }

    pub fn abs(&self) -> PathBuf {
        self.absolute.clone()
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
