use std::io::Error as IoError;

use crate::domain::object::key::ObjectKey;
use crate::domain::object::version::ObjectVersion;
use postcard::Error as PostcardError;
use uuid::Error as UuidError;
use serde::Serialize;
use thiserror::Error;

pub type So3Result<T> = Result<T, So3Error>;

#[derive(Debug, Error, Serialize)]
#[serde(tag = "kind", content = "detail")]
pub enum So3Error {
    #[error("object key must not be empty")]
    InvalidKey,
    #[error("invalid object version: {0}")]
    InvalidVersion(i64),
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("cas mismatch for key {key}: expected {expected}, actual {actual}")]
    CasMismatch {
        key: String,
        expected: i64,
        actual: i64,
    },
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("repository error: {0}")]
    Storage(String),
    #[error("i/o error: {0}")]
    Io(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    /// Transient failure contacting a consensus peer; safe to retry the operation.
    #[error("peer unavailable: {0}")]
    PeerUnavailable(String),
}

impl So3Error {
    #[must_use]
    pub fn not_found(key: &ObjectKey) -> Self {
        Self::NotFound(key.as_ref().to_owned())
    }

    #[must_use]
    pub fn cas_mismatch(key: &ObjectKey, expected: ObjectVersion, actual: ObjectVersion) -> Self {
        Self::CasMismatch {
            key: key.as_ref().to_owned(),
            expected: expected.get(),
            actual: actual.get(),
        }
    }
}

impl From<IoError> for So3Error {
    fn from(value: IoError) -> Self {
        Self::Io(value.to_string())
    }
}

impl From<PostcardError> for So3Error {
    fn from(value: PostcardError) -> Self {
        Self::Serialization(value.to_string())
    }
}

impl From<UuidError> for So3Error {
    fn from(value: UuidError) -> Self {
        Self::Io(value.to_string())
    }
}
