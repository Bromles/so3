use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    pub fn digest_bytes(data: &[u8]) -> Self {
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
        Self::digest_bytes(value.as_bytes())
    }
}
