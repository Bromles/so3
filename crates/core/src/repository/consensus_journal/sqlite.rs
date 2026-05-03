use crate::domain::clock::LogicalTimestamp;
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::consensus::ballot::Ballot;
use crate::domain::consensus::command_id::{CommandId, DependencySet};
use crate::domain::consensus::journal::{JournalEntry, JournalState};
use crate::domain::error::So3Result;
use crate::domain::node::NodeId;
use crate::repository::consensus_journal::ConsensusJournalRepository;
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
impl ConsensusJournalRepository for SqliteConsensusJournal {
    async fn load(&self, _command_id: &CommandId) -> So3Result<Option<JournalEntry>> {
        todo!()
    }

    async fn check_conflicts(&self, _command_id: &CommandId) -> So3Result<Vec<CommandId>> {
        todo!()
    }

    async fn record_pre_accepted(
        &self,
        _command_id: &CommandId,
        _command: &ObjectCommand,
        _timestamp_zero: &LogicalTimestamp,
        _deps: &DependencySet,
    ) -> So3Result<()> {
        todo!()
    }

    async fn record_accepted(
        &self,
        _command_id: &CommandId,
        _ballot: &Ballot,
        _timestamp: &LogicalTimestamp,
    ) -> So3Result<()> {
        todo!()
    }

    async fn record_committed(&self, _command_id: &CommandId) -> So3Result<()> {
        todo!()
    }

    async fn record_applied(
        &self,
        _command_id: &CommandId,
        _result: &CommandResult,
    ) -> So3Result<()> {
        todo!()
    }

    async fn list_by_state(&self, _state: JournalState) -> So3Result<Vec<JournalEntry>> {
        todo!()
    }

    async fn max_sequence(&self, _node_id: &NodeId) -> So3Result<u64> {
        todo!()
    }
}
