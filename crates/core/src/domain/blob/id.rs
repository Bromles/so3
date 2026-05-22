use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::error::So3Error;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobId(String);

impl BlobId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn from_sha256(digest: &Sha256Digest) -> Self {
        Self(digest.to_hex())
    }
}

impl TryFrom<&str> for BlobId {
    type Error = So3Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s.trim().is_empty() {
            Err(So3Error::InvalidRequest(
                "blob id must not be empty".to_string(),
            ))
        } else {
            Ok(Self(s.to_string()))
        }
    }
}

impl std::fmt::Display for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
