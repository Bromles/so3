use bytes::Bytes;

#[derive(Clone)]
pub struct BlobPayload(Bytes);

impl BlobPayload {
    pub fn new(bytes: Bytes) -> Self {
        Self(bytes)
    }
    pub fn from_vec(vec: Vec<u8>) -> Self {
        Self(Bytes::from(vec))
    }
    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
}
