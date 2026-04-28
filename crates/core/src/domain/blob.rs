use crate::domain::checksum::Sha256Digest;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BlobId(String);

impl Deref for BlobId {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for BlobId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<String> for BlobId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMetadata {
    pub blob_id: BlobId,
    pub content_length: u64,
    pub checksum_sha256: Sha256Digest,
}

pub struct BlobPayload(Vec<u8>);

impl Deref for BlobPayload {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for BlobPayload {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Vec<u8>> for BlobPayload {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

pub struct Blob {
    pub metadata: BlobMetadata,
    pub payload: BlobPayload,
}
