use crate::domain::consensus::command_id::CommandId;
use crate::domain::consensus::journal::{JournalEntry, JournalMetadata};
use crate::domain::consensus::transport::RecoveryState;
use crate::domain::error::So3Result;
use async_trait::async_trait;

#[async_trait]
pub trait ConsensusJournal {
    async fn load(&self, command_id: &CommandId) -> So3Result<Option<JournalEntry>>;
    async fn list_by_state(&self, state: RecoveryState) -> So3Result<Vec<JournalEntry>>;
    async fn next_sequence_for_origin(&self, origin_node_id: &str) -> So3Result<u64>;
    async fn record_pre_accepted(
        &self,
        command_id: &CommandId,
        command: &[u8],
        metadata: JournalMetadata,
    ) -> So3Result<JournalEntry>;
    async fn record_accepted(
        &self,
        command_id: &CommandId,
        command: &[u8],
        metadata: JournalMetadata,
    ) -> So3Result<JournalEntry>;
    async fn record_committed(
        &self,
        command_id: &CommandId,
        command: &[u8],
        metadata: JournalMetadata,
    ) -> So3Result<JournalEntry>;
    async fn record_applied(
        &self,
        command_id: &CommandId,
        command: &[u8],
        result: &[u8],
        metadata: JournalMetadata,
    ) -> So3Result<JournalEntry>;
}
