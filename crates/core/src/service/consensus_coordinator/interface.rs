use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::error::So3Result;
use async_trait::async_trait;

#[async_trait]
pub trait ConsensusCoordinatorService: Send + Sync + 'static {
    async fn coordinate(&self, command: ObjectCommand) -> So3Result<CommandResult>;
}
