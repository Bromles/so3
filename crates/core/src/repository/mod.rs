use crate::domain::error::So3Error;
use sqlx::Error as SqlxError;

pub mod blob;
pub mod consensus_journal;
pub mod metadata;
pub mod node_identity;
pub mod registry;

impl From<SqlxError> for So3Error {
    fn from(value: SqlxError) -> Self {
        Self::Storage(value.to_string())
    }
}
