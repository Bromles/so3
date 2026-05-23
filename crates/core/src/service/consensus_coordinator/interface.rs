use crate::domain::clock::LogicalTimestamp;
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::consensus::command_id::CommandId;
use crate::domain::consensus::transport::ApplyRequest;
use crate::domain::error::So3Result;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
use async_trait::async_trait;

pub enum BufferedEntry {
    Write(ObjectMetadata),
    Deleted,
}

#[async_trait]
pub trait ConsensusCoordinatorService: Send + Sync + 'static {
    async fn coordinate(&self, command: ObjectCommand) -> So3Result<CommandResult>;
    async fn apply(&self, req: ApplyRequest) -> So3Result<CommandResult>;
    fn register_committed(
        &self,
        key: ObjectKey,
        timestamp: LogicalTimestamp,
        command_id: CommandId,
    );
    fn get_buffered_entry(&self, key: &ObjectKey) -> Option<BufferedEntry>;
}
