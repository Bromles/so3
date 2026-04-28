use async_trait::async_trait;

use crate::consensus::ConsensusCommandId;
use crate::domain::command::CommandResult;
use crate::domain::error::So3Result;

#[async_trait]
pub trait AppliedCommandRepository: Send + Sync {
    /// # Errors
    ///
    /// Returns an error when the repository cannot load a previously applied replicated result.
    async fn load_result(
        &self,
        command_id: &ConsensusCommandId,
    ) -> So3Result<Option<CommandResult>>;

    /// # Errors
    ///
    /// Returns an error when the repository cannot persist the applied replicated result.
    async fn save_result(
        &self,
        command_id: &ConsensusCommandId,
        result: &CommandResult,
    ) -> So3Result<()>;
}
