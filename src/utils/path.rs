use crate::utils::errors::{AppError, InternalError};
use std::path::{Display, Path, PathBuf};

#[derive(Clone, Debug)]
pub struct RepoPath {
    relative: PathBuf,
    absolute: PathBuf,
}

impl RepoPath {
    pub fn from_root(root: impl Into<PathBuf>) -> Self {
        Self {
            relative: "".into(),
            absolute: root.into(),
        }
    }

    pub fn from_current<P: AsRef<Path>>(
        file_path: P,
        root: &RepoPath,
    ) -> Result<Self, InternalError> {
        let abs_file_path = std::path::absolute(file_path.as_ref()).map_err(InternalError::IO)?;
        let abs_repo_root = std::path::absolute(root).map_err(InternalError::IO)?;
        let rel_candidate = abs_file_path.strip_prefix(&abs_repo_root).map_err(|_| {
            AppError::FileNotPartOfRepository(abs_file_path.to_string_lossy().into())
        })?;
        Ok(root.join(rel_candidate))
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
    use super::*;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn test_repo_path_join() {
        let root = PathBuf::from("/my/repo");
        let repo_path = RepoPath::from_root(root.clone());
        let sub = repo_path.join("subdir/file.txt");
        assert_eq!(sub.rel().to_string_lossy(), "subdir/file.txt");
        assert_eq!(sub.abs().to_string_lossy(), "/my/repo/subdir/file.txt");
    }

    #[test]
    fn test_from_current() -> Result<(), InternalError> {
        let original_dir = std::env::current_dir().map_err(InternalError::IO)?;

        // Given
        let temp_repo = tempdir().map_err(InternalError::IO)?;
        let repo = RepoPath::from_root(temp_repo.path().canonicalize()?);
        std::env::set_current_dir(&repo).map_err(InternalError::IO)?;

        // When
        let result = RepoPath::from_current("subdir/file.txt", &repo)?;

        // Then
        let expected = repo.join("subdir/file.txt");
        assert_eq!(result.abs(), expected.abs());
        assert_eq!(result.rel(), expected.rel());

        // Restore
        std::env::set_current_dir(original_dir).map_err(InternalError::IO)?;
        Ok(())
    }

    #[test]
    fn test_from_current_subdir() -> Result<(), InternalError> {
        let original_dir = std::env::current_dir().map_err(InternalError::IO)?;

        // Given
        let temp_repo = tempdir().map_err(InternalError::IO)?;
        let repo = RepoPath::from_root(temp_repo.path().canonicalize()?);
        std::fs::create_dir_all(repo.join("subdir"))?;
        std::env::set_current_dir(repo.join("subdir")).map_err(InternalError::IO)?;

        // When
        let result = RepoPath::from_current("file.txt", &repo)?;

        // Then
        let expected = repo.join("subdir/file.txt");
        assert_eq!(result.abs(), expected.abs());
        assert_eq!(result.rel(), expected.rel());

        // Restore
        std::env::set_current_dir(original_dir).map_err(InternalError::IO)?;
        Ok(())
    }

    #[test]
    fn test_from_current_not_in_repo() -> Result<(), InternalError> {
        // Given
        let temp_repo = tempdir().map_err(InternalError::IO)?;
        let repo = RepoPath::from_root(temp_repo.path());

        let file_path = PathBuf::from("/not-part-of-repo.txt");

        // Then
        let result = RepoPath::from_current(file_path, &repo);
        assert!(result.is_err());

        Ok(())
    }
}
