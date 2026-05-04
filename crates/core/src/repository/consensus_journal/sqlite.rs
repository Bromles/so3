use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{query, query_scalar, Row, SqlitePool};
use std::path::Path;
use std::time::Duration;
use tokio::fs;

use crate::domain::clock::LogicalTimestamp;
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::consensus::ballot::Ballot;
use crate::domain::consensus::command_id::{CommandId, DependencySet};
use crate::domain::consensus::journal::{JournalEntry, JournalState};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::node::NodeId;
use crate::repository::consensus_journal::mappers::{
    command_key, encode_command, encode_deps, encode_result, i64_to_u64, row_to_entry,
    sequence_to_i64,
};
use crate::repository::consensus_journal::ConsensusJournalRepository;

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_MAX_CONNECTIONS: u32 = 1;
const DATABASE_FILE_NAME: &str = "consensus.sqlite";

const SELECT_COLS: &str = "origin_node_id, sequence, state, command, deps, \
     t0_epoch, t0_physical_ms, t0_logical, t0_node_id, \
     t_epoch, t_physical_ms, t_logical, t_node_id, \
     ballot_round, ballot_node_id, result";

const CREATE_JOURNAL_SQL: &str = r"
    CREATE TABLE IF NOT EXISTS consensus_journal (
        origin_node_id   TEXT    NOT NULL,
        sequence         INTEGER NOT NULL,
        state            INTEGER NOT NULL,
        key              TEXT    NOT NULL,
        command          BLOB    NOT NULL,
        deps             BLOB    NOT NULL,
        t0_epoch         INTEGER NOT NULL,
        t0_physical_ms   INTEGER NOT NULL,
        t0_logical       INTEGER NOT NULL,
        t0_node_id       TEXT    NOT NULL,
        t_epoch          INTEGER,
        t_physical_ms    INTEGER,
        t_logical        INTEGER,
        t_node_id        TEXT,
        ballot_round     INTEGER,
        ballot_node_id   TEXT,
        result           BLOB,
        PRIMARY KEY (origin_node_id, sequence)
    )
";

pub struct SqliteConsensusJournal {
    pool: SqlitePool,
}

impl SqliteConsensusJournal {
    pub async fn new(dir: impl AsRef<Path>) -> So3Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).await?;

        let options = SqliteConnectOptions::new()
            .filename(dir.join(DATABASE_FILE_NAME))
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(SQLITE_BUSY_TIMEOUT);

        let pool = SqlitePoolOptions::new()
            .max_connections(SQLITE_MAX_CONNECTIONS)
            .connect_with(options)
            .await?;

        query(CREATE_JOURNAL_SQL).execute(&pool).await?;

        Ok(Self { pool })
    }

    fn ts_to_i64s(ts: &LogicalTimestamp) -> So3Result<(i64, i64, i64)> {
        let epoch =
            i64::try_from(ts.epoch).map_err(|_| So3Error::Storage("epoch overflow".into()))?;
        let physical = i64::try_from(ts.physical_millis)
            .map_err(|_| So3Error::Storage("physical_millis overflow".into()))?;
        let logical =
            i64::try_from(ts.logical).map_err(|_| So3Error::Storage("logical overflow".into()))?;
        Ok((epoch, physical, logical))
    }
}

#[async_trait]
impl ConsensusJournalRepository for SqliteConsensusJournal {
    async fn load(&self, command_id: &CommandId) -> So3Result<Option<JournalEntry>> {
        let seq = sequence_to_i64(command_id.sequence)?;
        let row = query(&format!(
            "SELECT {SELECT_COLS} FROM consensus_journal \
             WHERE origin_node_id = ? AND sequence = ?"
        ))
        .bind(command_id.origin_node_id.as_ref())
        .bind(seq)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(row_to_entry).transpose()
    }

    async fn check_conflicts_and_record_pre_accepted(
        &self,
        command_id: &CommandId,
        command: &ObjectCommand,
        timestamp_zero: &LogicalTimestamp,
    ) -> So3Result<DependencySet> {
        let seq = sequence_to_i64(command_id.sequence)?;
        let (t0_epoch, t0_physical, t0_logical) = Self::ts_to_i64s(timestamp_zero)?;

        // BEGIN IMMEDIATE acquires the write lock before we read, so two concurrent
        // coordinators cannot both complete the conflict check before either inserts.
        // Transaction rolls back automatically on drop if not committed.
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        let rows = query(
            "SELECT origin_node_id, sequence FROM consensus_journal \
             WHERE key = ? AND state < ? \
               AND NOT (origin_node_id = ? AND sequence = ?)",
        )
        .bind(command_key(command))
        .bind(JournalState::Applied.as_i32())
        .bind(command_id.origin_node_id.as_ref())
        .bind(seq)
        .fetch_all(&mut *tx)
        .await?;

        let deps: Vec<CommandId> = rows
            .iter()
            .map(|r| {
                let node_id: String = r.try_get("origin_node_id")?;
                let s: i64 = r.try_get("sequence")?;
                Ok(CommandId {
                    origin_node_id: NodeId::new(node_id),
                    sequence: i64_to_u64(s, "sequence")?,
                })
            })
            .collect::<So3Result<_>>()?;

        query(
            "INSERT OR REPLACE INTO consensus_journal \
             (origin_node_id, sequence, state, key, command, deps, \
              t0_epoch, t0_physical_ms, t0_logical, t0_node_id) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(command_id.origin_node_id.as_ref())
        .bind(seq)
        .bind(JournalState::PreAccepted.as_i32())
        .bind(command_key(command))
        .bind(encode_command(command)?)
        .bind(encode_deps(&deps)?)
        .bind(t0_epoch)
        .bind(t0_physical)
        .bind(t0_logical)
        .bind(timestamp_zero.node_id.as_ref())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(DependencySet(deps))
    }

    async fn record_accepted(
        &self,
        command_id: &CommandId,
        ballot: &Ballot,
        timestamp: &LogicalTimestamp,
    ) -> So3Result<()> {
        let seq = sequence_to_i64(command_id.sequence)?;
        let (t_epoch, t_physical, t_logical) = Self::ts_to_i64s(timestamp)?;
        let ballot_round = i64::try_from(ballot.round)
            .map_err(|_| So3Error::Storage("ballot round overflow".into()))?;

        let n = query(
            "UPDATE consensus_journal \
             SET state = ?, \
                 t_epoch = ?, t_physical_ms = ?, t_logical = ?, t_node_id = ?, \
                 ballot_round = ?, ballot_node_id = ? \
             WHERE origin_node_id = ? AND sequence = ?",
        )
        .bind(JournalState::Accepted.as_i32())
        .bind(t_epoch)
        .bind(t_physical)
        .bind(t_logical)
        .bind(timestamp.node_id.as_ref())
        .bind(ballot_round)
        .bind(ballot.node_id.as_ref())
        .bind(command_id.origin_node_id.as_ref())
        .bind(seq)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if n != 1 {
            return Err(So3Error::Storage(format!(
                "record_accepted: expected 1 row, got {n} for {command_id:?}"
            )));
        }
        Ok(())
    }

    async fn record_committed(&self, command_id: &CommandId) -> So3Result<()> {
        let seq = sequence_to_i64(command_id.sequence)?;
        let n = query(
            "UPDATE consensus_journal SET state = ? \
             WHERE origin_node_id = ? AND sequence = ?",
        )
        .bind(JournalState::Committed.as_i32())
        .bind(command_id.origin_node_id.as_ref())
        .bind(seq)
        .execute(&self.pool)
        .await?
        .rows_affected();
        
        if n != 1 {
            return Err(So3Error::Storage(format!(
                "record_committed: expected 1 row, got {n} for {command_id:?}"
            )));
        }
        
        Ok(())
    }

    async fn record_applied(
        &self,
        command_id: &CommandId,
        result: &CommandResult,
    ) -> So3Result<()> {
        let seq = sequence_to_i64(command_id.sequence)?;
        let n = query(
            "UPDATE consensus_journal SET state = ?, result = ? \
             WHERE origin_node_id = ? AND sequence = ?",
        )
        .bind(JournalState::Applied.as_i32())
        .bind(encode_result(result)?)
        .bind(command_id.origin_node_id.as_ref())
        .bind(seq)
        .execute(&self.pool)
        .await?
        .rows_affected();
        
        if n != 1 {
            return Err(So3Error::Storage(format!(
                "record_applied: expected 1 row, got {n} for {command_id:?}"
            )));
        }
        
        Ok(())
    }

    async fn list_by_state(&self, state: JournalState) -> So3Result<Vec<JournalEntry>> {
        let rows = query(&format!(
            "SELECT {SELECT_COLS} FROM consensus_journal WHERE state = ?"
        ))
        .bind(state.as_i32())
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(row_to_entry).collect()
    }

    async fn max_sequence(&self, node_id: &NodeId) -> So3Result<u64> {
        let max: Option<i64> =
            query_scalar("SELECT MAX(sequence) FROM consensus_journal WHERE origin_node_id = ?")
                .bind(node_id.as_ref())
                .fetch_one(&self.pool)
                .await?;

        match max {
            None => Ok(0),
            Some(s) => i64_to_u64(s, "max_sequence"),
        }
    }
}
