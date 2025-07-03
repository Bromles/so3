use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac, digest::MacError};
use sha2::Sha256;

use crate::signature::Credentials;

type HmacSha256 = Hmac<Sha256>;

pub struct SignRequestInput {
    payload_hash: Vec<u8>,
    credentials: Credentials,
    service: String,
    region: String,
    time: DateTime<Utc>,
}

fn encode_hmac_sha256(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> HmacSha256 {
    let mut mac = HmacSha256::new_from_slice(key.as_ref()).unwrap();
    mac.update(data.as_ref());
    mac
}

fn verify_hmac_sha256(
    mac: &mut HmacSha256,
    data: impl AsRef<[u8]>,
    code_bytes: impl AsRef<[u8]>,
) -> Result<(), MacError> {
    mac.update(data.as_ref());

    mac.clone().verify_slice(code_bytes.as_ref())
}
