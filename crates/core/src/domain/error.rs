use std::io::Error as IoError;

use postcard::Error as PostcardError;
use serde::Serialize;
use sqlx::Error as SqlxError;
use thiserror::Error;

use crate::domain::{ObjectKey, ObjectVersion};

pub type So3Result<T> = std::result::Result<T, So3Error>;

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
    #[error("storage error: {0}")]
    Storage(String),
    #[error("i/o error: {0}")]
    Io(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("rpc server is not implemented yet")]
    RpcNotImplemented,
    /// Transient failure contacting a consensus peer; safe to retry the operation.
    #[error("peer unavailable: {0}")]
    PeerUnavailable(String),
}

impl So3Error {
    #[must_use]
    pub fn not_found(key: &ObjectKey) -> Self {
        Self::NotFound(key.as_str().to_owned())
    }

    #[must_use]
    pub fn cas_mismatch(key: &ObjectKey, expected: ObjectVersion, actual: ObjectVersion) -> Self {
        Self::CasMismatch {
            key: key.as_str().to_owned(),
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

impl From<SqlxError> for So3Error {
    fn from(value: SqlxError) -> Self {
        Self::Storage(value.to_string())
    }
}

impl From<PostcardError> for So3Error {
    fn from(value: PostcardError) -> Self {
        Self::Serialization(value.to_string())
    }
}
