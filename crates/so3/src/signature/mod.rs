use chrono::{DateTime, Utc};
use serde::Deserialize;

pub mod payload;
pub mod v4;

#[derive(Deserialize)]
pub struct Credentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    expires: DateTime<Utc>,
}
