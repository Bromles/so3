use crate::domain::consensus::command_id::CommandId;
use crate::domain::consensus::journal::{JournalEntry, JournalMetadata};
use crate::domain::consensus::transport::RecoveryState;
use crate::domain::error::So3Result;
use crate::repository::consensus_journal::ConsensusJournal;
use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;
use std::path::Path;
use std::time::Duration;
use tokio::fs;

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_MAX_CONNECTIONS: u32 = 1;
const DATABASE_FILE_NAME: &str = "consensus.sqlite";

pub struct SqliteConsensusJournal {
    pool: SqlitePool,
}

impl SqliteConsensusJournal {
    pub async fn new(dir: impl AsRef<Path>) -> So3Result<Self> {
        let data_dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir).await?;

        let options = SqliteConnectOptions::new()
            .filename(data_dir.join(DATABASE_FILE_NAME))
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(SQLITE_BUSY_TIMEOUT);

        let pool = SqlitePoolOptions::new()
            .max_connections(SQLITE_MAX_CONNECTIONS)
            .connect_with(options)
            .await?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl ConsensusJournal for SqliteConsensusJournal {
    async fn load(&self, command_id: &CommandId) -> So3Result<Option<JournalEntry>> {
        /*let row = query(LOAD_COMMAND_SQL)
            .bind(command_id.origin_node_id())
            .bind(sequence_to_i64(command_id.sequence())?)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            Some(row) => row_to_entry(&row).map(Some),
            None => Ok(None),
        }*/

        unimplemented!()
    }

    async fn list_by_state(&self, state: RecoveryState) -> So3Result<Vec<JournalEntry>> {
        /*let rows = query(LIST_COMMANDS_BY_STATE_SQL)
            .bind(state.as_sql())
            .fetch_all(&self.pool)
            .await?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            entries.push(row_to_entry(&row)?);
        }
        Ok(entries)*/

        unimplemented!()
    }

    async fn next_sequence_for_origin(&self, origin_node_id: &str) -> So3Result<u64> {
        todo!()
    }

    async fn record_pre_accepted(&self, command_id: &CommandId, command: &[u8], metadata: JournalMetadata) -> So3Result<JournalEntry> {
        todo!()
    }

    async fn record_accepted(&self, command_id: &CommandId, command: &[u8], metadata: JournalMetadata) -> So3Result<JournalEntry> {
        todo!()
    }

    async fn record_committed(&self, command_id: &CommandId, command: &[u8], metadata: JournalMetadata) -> So3Result<JournalEntry> {
        todo!()
    }

    async fn record_applied(&self, command_id: &CommandId, command: &[u8], result: &[u8], metadata: JournalMetadata) -> So3Result<JournalEntry> {
        todo!()
    }
}
