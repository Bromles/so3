use crate::domain::error::So3Error;
use bytes::Bytes;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    pub fn compute(data: &[u8]) -> Self {
        let digest = Sha256::digest(data);
        let bytes: [u8; 32] = digest.into();

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
