use async_trait::async_trait;
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::error::So3Result;

#[async_trait]
pub trait LocalStateMachine {
    async fn execute(&self, command: ObjectCommand) -> So3Result<CommandResult>;
}