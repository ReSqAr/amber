use std::fmt;

#[derive(Debug)]
pub enum InvariableError {
    NotInitialised(),
    FileMissing(String),
}

impl fmt::Display for InvariableError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InvariableError::NotInitialised() => write!(f, "Wasn't initialized yet"),
            InvariableError::FileMissing(name) => write!(f, "File is missing: {}", name),
        }
    }
}

impl std::error::Error for InvariableError {}
