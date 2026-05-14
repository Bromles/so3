use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
