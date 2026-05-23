use crate::domain::error::So3Error;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub struct Sha256Hasher(Sha256);

impl Default for Sha256Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256Hasher {
    pub fn new() -> Self {
        Self(Sha256::new())
    }

    pub fn update(&mut self, data: &[u8]) {
        Digest::update(&mut self.0, data);
    }

    pub fn finalize(self) -> Sha256Digest {
        Sha256Digest::from_bytes(Digest::finalize(self.0).into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    pub fn compute(data: &[u8]) -> Self {
        let digest = Sha256::digest(data);
        let bytes: [u8; 32] = digest.into();

        Self(bytes)
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        const_hex::encode(self.0)
    }
}

impl From<&str> for Sha256Digest {
    fn from(value: &str) -> Self {
        Self::compute(value.as_bytes())
    }
}

impl TryFrom<Bytes> for Sha256Digest {
    type Error = So3Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let bytes: [u8; 32] = value.as_ref().try_into().map_err(|_| {
            So3Error::InvalidRequest(format!("sha256 must be 32 bytes, got {}", value.len()))
        })?;

        Ok(Self(bytes))
    }
}
