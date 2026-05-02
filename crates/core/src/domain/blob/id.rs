use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BlobId(String);

impl BlobId {
    pub fn new() -> Self { Self(uuid::Uuid::new_v4().to_string()) }
    pub fn from_string(s: String) -> Self { Self(s) }
}

impl AsRef<str> for BlobId {
    fn as_ref(&self) -> &str { &self.0 }
}