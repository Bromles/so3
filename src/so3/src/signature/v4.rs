use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac, digest::MacError};
use http::HeaderName;
use sha2::Sha256;

use crate::signature::Credentials;

pub const X_AMZ_ALGORITHM_NAME: HeaderName = HeaderName::from_static("x-amz-algorithm");
pub const X_AMZ_CREDENTIAL_NAME: HeaderName = HeaderName::from_static("x-amz-credential");
pub const X_AMZ_DATE_NAME: HeaderName = HeaderName::from_static("x-amz-date");
pub const X_AMZ_EXPIRES_NAME: HeaderName = HeaderName::from_static("x-amz-expires");
pub const X_AMZ_SIGNATURE_NAME: HeaderName = HeaderName::from_static("x-amz-signature");
pub const X_AMZ_SIGNED_HEADERS_NAME: HeaderName = HeaderName::from_static("x-amz-signed-headers");
pub const X_AMZ_TRAILER_NAME: HeaderName = HeaderName::from_static("x-amz-trailer");

pub const EMPTY_BODY_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

pub const AWS4_HMAC_SHA256_NAME: &str = "AWS4-HMAC-SHA256";
type HmacSha256 = Hmac<Sha256>;

pub enum ContentSha256Header {
    UnsignedPayload,
    Sha256Checksum(Vec<u8>),
    StreamingPayload { trailer: bool, signed: bool },
}

impl ContentSha256Header {
    pub const UNSIGNED_PAYLOAD_NAME: &str = "UNSIGNED-PAYLOAD";
    pub const STREAMING_UNSIGNED_PAYLOAD_TRAILER_NAME: &str = "STREAMING_UNSIGNED_PAYLOAD_TRAILER";
    pub const STREAMING_AWS4_HMAC_SHA256_PAYLOAD_NAME: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
}

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
