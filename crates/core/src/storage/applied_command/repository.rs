use async_trait::async_trait;

use crate::consensus::ConsensusCommandId;
use crate::domain::ObjectResult;
use crate::domain::error::So3Result;

#[async_trait]
pub trait AppliedCommandStore: Send + Sync {
    /// # Errors
    ///
    /// Returns an error when the store cannot load a previously applied replicated result.
    async fn load_result(&self, command_id: &ConsensusCommandId)
    -> So3Result<Option<ObjectResult>>;

    /// # Errors
    ///
    /// Returns an error when the store cannot persist the applied replicated result.
    async fn save_result(
        &self,
        command_id: &ConsensusCommandId,
        result: &ObjectResult,
    ) -> So3Result<()>;
}
