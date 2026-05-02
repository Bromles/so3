use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobId(Uuid);

impl BlobId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl TryFrom<&str> for BlobId {
    type Error = uuid::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Ok(Self(s.parse()?))
    }
}

impl std::fmt::Display for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
