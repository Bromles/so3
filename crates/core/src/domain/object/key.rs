use crate::domain::error::{So3Error, So3Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn new(key: impl Into<String>) -> So3Result<Self> {
        let s = key.into();

        if s.is_empty() {
            return Err(So3Error::InvalidKey);
        }

        Ok(Self(s))
    }
}

impl AsRef<str> for ObjectKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
