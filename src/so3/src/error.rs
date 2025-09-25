use std::fmt::Display;

use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    Default,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Default => write!(f, "Default error"),
        }
    }
}
