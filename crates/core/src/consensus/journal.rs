use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use prost::Message as ProstMessage;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use sqlx::{Row, SqlitePool, query, query_scalar};
use tokio::fs;
use tokio::sync::Mutex;

use crate::consensus::ConsensusCommandId;
use crate::domain::error::{So3Error, So3Result};
use crate::rpc_server::proto::{Ballot, DependencySet, LogicalTimestamp};

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_MAX_CONNECTIONS: u32 = 1;
const DATABASE_FILE_NAME: &str = "consensus.sqlite";
const CURRENT_SCHEMA_VERSION: i64 = 3;
const EMPTY_RESULT_BYTES: &[u8] = b"";
const STATE_PRE_ACCEPTED: i64 = 1;
const STATE_ACCEPTED: i64 = 2;
const STATE_COMMITTED: i64 = 3;
const STATE_APPLIED: i64 = 4;

const COMMANDS_TABLE_SQL: &str = r"
    CREATE TABLE IF NOT EXISTS command_journal (
        origin_node_id TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        state INTEGER NOT NULL,
        command BLOB NOT NULL,
        result BLOB NOT NULL,
        PRIMARY KEY (origin_node_id, sequence)
    )
";
const LOAD_COMMAND_SQL: &str = r"
    SELECT origin_node_id, sequence, state, command, result,
           timestamp_zero, timestamp, dependencies, ballot
    FROM command_journal
    WHERE origin_node_id = ? AND sequence = ?
";
const NEXT_SEQUENCE_SQL: &str = r"
    SELECT COALESCE(MAX(sequence), 0) + 1
    FROM command_journal
    WHERE origin_node_id = ?
";
const LIST_COMMANDS_BY_STATE_SQL: &str = r"
    SELECT origin_node_id, sequence, state, command, result,
           timestamp_zero, timestamp, dependencies, ballot
    FROM command_journal
    WHERE state = ?
    ORDER BY origin_node_id, sequence
";
const INSERT_APPLIED_COMMAND_SQL: &str = r"
    INSERT INTO command_journal (
        origin_node_id, sequence, state, command, result,
        timestamp_zero, timestamp, dependencies, ballot
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
";
const UPDATE_COMMAND_SQL: &str = r"
    UPDATE command_journal
    SET state = ?, command = ?, result = ?,
        timestamp_zero = ?, timestamp = ?, dependencies = ?, ballot = ?
    WHERE origin_node_id = ? AND sequence = ?
";
const ADD_TIMESTAMP_ZERO_SQL: &str = r"
    ALTER TABLE command_journal
    ADD COLUMN timestamp_zero BLOB NOT NULL DEFAULT X''
";
const ADD_TIMESTAMP_SQL: &str = r"
    ALTER TABLE command_journal
    ADD COLUMN timestamp BLOB NOT NULL DEFAULT X''
";
const ADD_DEPENDENCIES_SQL: &str = r"
    ALTER TABLE command_journal
    ADD COLUMN dependencies BLOB NOT NULL DEFAULT X''
";
const ADD_BALLOT_SQL: &str = r"
    ALTER TABLE command_journal
    ADD COLUMN ballot BLOB NOT NULL DEFAULT X''
";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalState {
    PreAccepted,
    Accepted,
    Committed,
    Applied,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JournalEntry {
    pub command_id: ConsensusCommandId,
    pub state: JournalState,
    pub command: Vec<u8>,
    pub result: Vec<u8>,
    pub metadata: JournalMetadata,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct JournalMetadata {
    pub timestamp_zero: Option<LogicalTimestamp>,
    pub timestamp: Option<LogicalTimestamp>,
    pub dependencies: DependencySet,
    pub ballot: Option<Ballot>,
}

#[derive(Clone, Debug)]
pub struct SqliteConsensusJournal {
    pool: SqlitePool,
    write_lock: Arc<Mutex<()>>,
}

impl SqliteConsensusJournal {
    /// # Errors
    ///
    /// Returns an error if the journal database cannot be created, opened, or migrated.
    pub async fn new(data_dir: impl AsRef<Path>) -> So3Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
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

        let journal = Self {
            pool,
            write_lock: Arc::new(Mutex::new(())),
        };
        journal.init_schema().await?;
        Ok(journal)
    }

    async fn init_schema(&self) -> So3Result<()> {
        let version = query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&self.pool)
            .await?;

        match version {
            0 => {
                self.migrate_to_v1().await?;
                self.migrate_to_v2().await?;
                self.migrate_to_v3().await
            }
            1 => {
                self.migrate_to_v2().await?;
                self.migrate_to_v3().await
            }
            2 => self.migrate_to_v3().await,
            CURRENT_SCHEMA_VERSION => Ok(()),
            unsupported => Err(So3Error::Storage(format!(
                "unsupported consensus sqlite schema version: {unsupported}"
            ))),
        }
    }

    async fn migrate_to_v1(&self) -> So3Result<()> {
        query(COMMANDS_TABLE_SQL).execute(&self.pool).await?;
        query("PRAGMA user_version = 1").execute(&self.pool).await?;
        Ok(())
    }

    async fn migrate_to_v2(&self) -> So3Result<()> {
        query(ADD_TIMESTAMP_ZERO_SQL).execute(&self.pool).await?;
        query(ADD_TIMESTAMP_SQL).execute(&self.pool).await?;
        query(ADD_DEPENDENCIES_SQL).execute(&self.pool).await?;
        query("PRAGMA user_version = 2").execute(&self.pool).await?;
        Ok(())
    }

    async fn migrate_to_v3(&self) -> So3Result<()> {
        query(ADD_BALLOT_SQL).execute(&self.pool).await?;
        query(&format!("PRAGMA user_version = {CURRENT_SCHEMA_VERSION}"))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// # Errors
    ///
    /// Returns an error when the journal cannot load persisted command state.
    pub async fn load(&self, command_id: &ConsensusCommandId) -> So3Result<Option<JournalEntry>> {
        let row = query(LOAD_COMMAND_SQL)
            .bind(command_id.origin_node_id())
            .bind(sequence_to_i64(command_id.sequence())?)
            .fetch_optional(&self.pool)
            .await?;

        row.as_ref().map(row_to_entry).transpose()
    }

    /// # Errors
    ///
    /// Returns an error when the journal cannot enumerate persisted command state.
    pub async fn list_by_state(&self, state: JournalState) -> So3Result<Vec<JournalEntry>> {
        let rows = query(LIST_COMMANDS_BY_STATE_SQL)
            .bind(state.as_sql())
            .fetch_all(&self.pool)
            .await?;

        rows.iter().map(row_to_entry).collect()
    }

    /// # Errors
    ///
    /// Returns an error when the journal cannot read the durable command sequence floor.
    pub async fn next_sequence_for_origin(&self, origin_node_id: &str) -> So3Result<u64> {
        let sequence = query_scalar::<_, i64>(NEXT_SEQUENCE_SQL)
            .bind(origin_node_id)
            .fetch_one(&self.pool)
            .await?;

        i64_to_u64_sequence(sequence)
    }

    /// # Errors
    ///
    /// Returns an error when the pre-accepted command cannot be durably recorded.
    pub async fn record_pre_accepted(
        &self,
        command_id: &ConsensusCommandId,
        command: &[u8],
    ) -> So3Result<JournalEntry> {
        self.record_pre_accepted_with_metadata(command_id, command, JournalMetadata::default())
            .await
    }

    /// # Errors
    ///
    /// Returns an error when the pre-accepted command cannot be durably recorded.
    pub async fn record_pre_accepted_with_metadata(
        &self,
        command_id: &ConsensusCommandId,
        command: &[u8],
        metadata: JournalMetadata,
    ) -> So3Result<JournalEntry> {
        self.advance(
            command_id,
            JournalState::PreAccepted,
            command,
            EMPTY_RESULT_BYTES,
            &metadata,
        )
        .await
    }

    /// # Errors
    ///
    /// Returns an error when the accepted command cannot be durably recorded.
    pub async fn record_accepted(
        &self,
        command_id: &ConsensusCommandId,
        command: &[u8],
    ) -> So3Result<JournalEntry> {
        self.record_accepted_with_metadata(command_id, command, JournalMetadata::default())
            .await
    }

    /// # Errors
    ///
    /// Returns an error when the accepted command cannot be durably recorded.
    pub async fn record_accepted_with_metadata(
        &self,
        command_id: &ConsensusCommandId,
        command: &[u8],
        metadata: JournalMetadata,
    ) -> So3Result<JournalEntry> {
        self.advance(
            command_id,
            JournalState::Accepted,
            command,
            EMPTY_RESULT_BYTES,
            &metadata,
        )
        .await
    }

    /// # Errors
    ///
    /// Returns an error when the committed command cannot be durably recorded.
    pub async fn record_committed(
        &self,
        command_id: &ConsensusCommandId,
        command: &[u8],
    ) -> So3Result<JournalEntry> {
        self.record_committed_with_metadata(command_id, command, JournalMetadata::default())
            .await
    }

    /// # Errors
    ///
    /// Returns an error when the committed command cannot be durably recorded.
    pub async fn record_committed_with_metadata(
        &self,
        command_id: &ConsensusCommandId,
        command: &[u8],
        metadata: JournalMetadata,
    ) -> So3Result<JournalEntry> {
        self.advance(
            command_id,
            JournalState::Committed,
            command,
            EMPTY_RESULT_BYTES,
            &metadata,
        )
        .await
    }

    /// # Errors
    ///
    /// Returns an error when the applied command cannot be durably recorded.
    pub async fn record_applied(
        &self,
        command_id: &ConsensusCommandId,
        command: &[u8],
        result: &[u8],
    ) -> So3Result<JournalEntry> {
        self.record_applied_with_metadata(command_id, command, result, JournalMetadata::default())
            .await
    }

    /// # Errors
    ///
    /// Returns an error when the applied command cannot be durably recorded.
    pub async fn record_applied_with_metadata(
        &self,
        command_id: &ConsensusCommandId,
        command: &[u8],
        result: &[u8],
        metadata: JournalMetadata,
    ) -> So3Result<JournalEntry> {
        self.advance(
            command_id,
            JournalState::Applied,
            command,
            result,
            &metadata,
        )
        .await
    }

    async fn advance(
        &self,
        command_id: &ConsensusCommandId,
        next_state: JournalState,
        command: &[u8],
        result: &[u8],
        metadata: &JournalMetadata,
    ) -> So3Result<JournalEntry> {
        let _guard = self.write_lock.lock().await;
        let Some(existing) = self.load(command_id).await? else {
            self.insert(command_id, next_state, command, result, metadata)
                .await?;
            return Ok(JournalEntry {
                command_id: command_id.clone(),
                state: next_state,
                command: command.to_vec(),
                result: result.to_vec(),
                metadata: metadata.clone(),
            });
        };

        if existing.command != command {
            return Err(So3Error::InvalidRequest(format!(
                "conflicting command payload for consensus command {}:{}",
                command_id.origin_node_id(),
                command_id.sequence()
            )));
        }

        if existing.state.rank() > next_state.rank() {
            return Ok(existing);
        }

        if existing.state == next_state
            && (next_state != JournalState::Applied || !existing.result.is_empty())
        {
            return Ok(existing);
        }

        let metadata = merge_metadata(&existing.metadata, metadata);
        self.update(command_id, next_state, command, result, &metadata)
            .await?;
        Ok(JournalEntry {
            command_id: command_id.clone(),
            state: next_state,
            command: command.to_vec(),
            result: result.to_vec(),
            metadata,
        })
    }

    async fn insert(
        &self,
        command_id: &ConsensusCommandId,
        state: JournalState,
        command: &[u8],
        result: &[u8],
        metadata: &JournalMetadata,
    ) -> So3Result<()> {
        query(INSERT_APPLIED_COMMAND_SQL)
            .bind(command_id.origin_node_id())
            .bind(sequence_to_i64(command_id.sequence())?)
            .bind(state.as_sql())
            .bind(command)
            .bind(result)
            .bind(encode_optional_proto(metadata.timestamp_zero.as_ref()))
            .bind(encode_optional_proto(metadata.timestamp.as_ref()))
            .bind(metadata.dependencies.encode_to_vec())
            .bind(encode_optional_proto(metadata.ballot.as_ref()))
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn update(
        &self,
        command_id: &ConsensusCommandId,
        state: JournalState,
        command: &[u8],
        result: &[u8],
        metadata: &JournalMetadata,
    ) -> So3Result<()> {
        query(UPDATE_COMMAND_SQL)
            .bind(state.as_sql())
            .bind(command)
            .bind(result)
            .bind(encode_optional_proto(metadata.timestamp_zero.as_ref()))
            .bind(encode_optional_proto(metadata.timestamp.as_ref()))
            .bind(metadata.dependencies.encode_to_vec())
            .bind(encode_optional_proto(metadata.ballot.as_ref()))
            .bind(command_id.origin_node_id())
            .bind(sequence_to_i64(command_id.sequence())?)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    #[cfg(test)]
    async fn set_schema_version_for_test(&self, version: i64) -> So3Result<()> {
        query(&format!("PRAGMA user_version = {version}"))
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

fn row_to_entry(row: &SqliteRow) -> So3Result<JournalEntry> {
    let sequence = row.try_get::<i64, _>("sequence")?;
    let state = row.try_get::<i64, _>("state")?;

    Ok(JournalEntry {
        command_id: ConsensusCommandId::new(
            row.try_get("origin_node_id")?,
            i64_to_u64_sequence(sequence)?,
        ),
        state: parse_state(state)?,
        command: row.try_get("command")?,
        result: row.try_get("result")?,
        metadata: JournalMetadata {
            timestamp_zero: decode_optional_proto(&row.try_get::<Vec<u8>, _>("timestamp_zero")?)?,
            timestamp: decode_optional_proto(&row.try_get::<Vec<u8>, _>("timestamp")?)?,
            dependencies: decode_dependencies(&row.try_get::<Vec<u8>, _>("dependencies")?)?,
            ballot: decode_optional_proto(&row.try_get::<Vec<u8>, _>("ballot")?)?,
        },
    })
}

fn merge_metadata(existing: &JournalMetadata, next: &JournalMetadata) -> JournalMetadata {
    JournalMetadata {
        timestamp_zero: next
            .timestamp_zero
            .clone()
            .or_else(|| existing.timestamp_zero.clone()),
        timestamp: next
            .timestamp
            .clone()
            .or_else(|| existing.timestamp.clone()),
        dependencies: if next.dependencies.commands.is_empty() {
            existing.dependencies.clone()
        } else {
            next.dependencies.clone()
        },
        ballot: merge_ballot(existing.ballot.as_ref(), next.ballot.as_ref()),
    }
}

fn merge_ballot(existing: Option<&Ballot>, next: Option<&Ballot>) -> Option<Ballot> {
    match (existing, next) {
        (Some(existing), Some(next)) => Some(max_ballot(existing, next).clone()),
        (None, Some(next)) => Some(next.clone()),
        (Some(existing), None) => Some(existing.clone()),
        (None, None) => None,
    }
}

fn max_ballot<'a>(left: &'a Ballot, right: &'a Ballot) -> &'a Ballot {
    if ballot_is_after(right, left) {
        right
    } else {
        left
    }
}

pub(crate) fn ballot_is_after(candidate: &Ballot, current: &Ballot) -> bool {
    candidate.round > current.round
        || (candidate.round == current.round && candidate.node_id > current.node_id)
}

fn encode_optional_proto<T: ProstMessage>(value: Option<&T>) -> Vec<u8> {
    value.map_or_else(Vec::new, ProstMessage::encode_to_vec)
}

fn decode_optional_proto<T>(bytes: &[u8]) -> So3Result<Option<T>>
where
    T: Default + ProstMessage,
{
    if bytes.is_empty() {
        return Ok(None);
    }

    T::decode(bytes)
        .map(Some)
        .map_err(|error| So3Error::Serialization(error.to_string()))
}

fn decode_dependencies(bytes: &[u8]) -> So3Result<DependencySet> {
    if bytes.is_empty() {
        return Ok(DependencySet {
            commands: Vec::new(),
        });
    }

    DependencySet::decode(bytes).map_err(|error| So3Error::Serialization(error.to_string()))
}

fn parse_state(state: i64) -> So3Result<JournalState> {
    match state {
        STATE_PRE_ACCEPTED => Ok(JournalState::PreAccepted),
        STATE_ACCEPTED => Ok(JournalState::Accepted),
        STATE_COMMITTED => Ok(JournalState::Committed),
        STATE_APPLIED => Ok(JournalState::Applied),
        unsupported => Err(So3Error::Storage(format!(
            "unsupported journal command state: {unsupported}"
        ))),
    }
}

impl JournalState {
    fn as_sql(self) -> i64 {
        match self {
            Self::PreAccepted => STATE_PRE_ACCEPTED,
            Self::Accepted => STATE_ACCEPTED,
            Self::Committed => STATE_COMMITTED,
            Self::Applied => STATE_APPLIED,
        }
    }

    fn rank(self) -> i64 {
        self.as_sql()
    }
}

fn sequence_to_i64(sequence: u64) -> So3Result<i64> {
    i64::try_from(sequence).map_err(|_| {
        So3Error::Storage(format!(
            "command sequence exceeds sqlite integer range: {sequence}"
        ))
    })
}

fn i64_to_u64_sequence(sequence: i64) -> So3Result<u64> {
    u64::try_from(sequence)
        .map_err(|_| So3Error::Storage(format!("invalid negative command sequence: {sequence}")))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{JournalMetadata, JournalState, SqliteConsensusJournal};
    use crate::consensus::ConsensusCommandId;
    use crate::domain::error::So3Error;
    use crate::rpc_server::proto::{Ballot, CommandId, DependencySet, LogicalTimestamp};

    const ORIGIN_NODE_ID: &str = "node-a";
    const COMMAND_SEQUENCE: u64 = 3;
    const COMMAND_BYTES: &[u8] = b"command";
    const RESULT_BYTES: &[u8] = b"result";
    const UNKNOWN_SCHEMA_VERSION: i64 = 99;
    const TIMESTAMP_EPOCH: u64 = 11;
    const TIMESTAMP_COUNTER: u64 = 12;

    fn command_id() -> ConsensusCommandId {
        ConsensusCommandId::new(ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE)
    }

    #[tokio::test]
    async fn record_then_load_roundtrips_applied_entry() {
        let temp_dir = TempDir::new().unwrap();
        let journal = SqliteConsensusJournal::new(temp_dir.path()).await.unwrap();

        journal
            .record_applied(&command_id(), COMMAND_BYTES, RESULT_BYTES)
            .await
            .unwrap();
        let entry = journal.load(&command_id()).await.unwrap().unwrap();

        assert_eq!(entry.command_id, command_id());
        assert_eq!(entry.state, JournalState::Applied);
        assert_eq!(entry.command, COMMAND_BYTES.to_vec());
        assert_eq!(entry.result, RESULT_BYTES.to_vec());
    }

    #[tokio::test]
    async fn record_survives_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let journal = SqliteConsensusJournal::new(temp_dir.path()).await.unwrap();

        journal
            .record_applied(&command_id(), COMMAND_BYTES, RESULT_BYTES)
            .await
            .unwrap();
        drop(journal);

        let reopened = SqliteConsensusJournal::new(temp_dir.path()).await.unwrap();
        let entry = reopened.load(&command_id()).await.unwrap().unwrap();

        assert_eq!(entry.result, RESULT_BYTES.to_vec());
    }

    #[tokio::test]
    async fn record_metadata_survives_reopen_and_later_state_transitions() {
        let temp_dir = TempDir::new().unwrap();
        let journal = SqliteConsensusJournal::new(temp_dir.path()).await.unwrap();
        let metadata = JournalMetadata {
            timestamp_zero: Some(LogicalTimestamp {
                epoch: TIMESTAMP_EPOCH,
                counter: TIMESTAMP_COUNTER,
                node_id: ORIGIN_NODE_ID.to_owned(),
            }),
            timestamp: Some(LogicalTimestamp {
                epoch: TIMESTAMP_EPOCH,
                counter: TIMESTAMP_COUNTER + 1,
                node_id: ORIGIN_NODE_ID.to_owned(),
            }),
            dependencies: DependencySet {
                commands: vec![CommandId {
                    origin_node_id: "node-b".to_owned(),
                    sequence: 2,
                }],
            },
            ballot: Some(Ballot {
                round: 7,
                node_id: "node-c".to_owned(),
            }),
        };

        let _ = journal
            .record_pre_accepted_with_metadata(&command_id(), COMMAND_BYTES, metadata.clone())
            .await
            .unwrap();
        let _ = journal
            .record_committed(&command_id(), COMMAND_BYTES)
            .await
            .unwrap();
        drop(journal);

        let reopened = SqliteConsensusJournal::new(temp_dir.path()).await.unwrap();
        let entry = reopened.load(&command_id()).await.unwrap().unwrap();

        assert_eq!(entry.state, JournalState::Committed);
        assert_eq!(entry.metadata, metadata);
    }

    #[tokio::test]
    async fn records_protocol_state_transitions_in_order() {
        let temp_dir = TempDir::new().unwrap();
        let journal = SqliteConsensusJournal::new(temp_dir.path()).await.unwrap();

        let pre_accepted = journal
            .record_pre_accepted(&command_id(), COMMAND_BYTES)
            .await
            .unwrap();
        let accepted = journal
            .record_accepted(&command_id(), COMMAND_BYTES)
            .await
            .unwrap();
        let committed = journal
            .record_committed(&command_id(), COMMAND_BYTES)
            .await
            .unwrap();

        assert_eq!(pre_accepted.state, JournalState::PreAccepted);
        assert_eq!(accepted.state, JournalState::Accepted);
        assert_eq!(committed.state, JournalState::Committed);
    }

    #[tokio::test]
    async fn list_by_state_returns_only_matching_entries() {
        let temp_dir = TempDir::new().unwrap();
        let journal = SqliteConsensusJournal::new(temp_dir.path()).await.unwrap();
        let accepted_id = ConsensusCommandId::new("node-a".to_owned(), 1);
        let committed_id = ConsensusCommandId::new("node-b".to_owned(), 2);

        let _ = journal
            .record_accepted(&accepted_id, COMMAND_BYTES)
            .await
            .unwrap();
        let _ = journal
            .record_committed(&committed_id, COMMAND_BYTES)
            .await
            .unwrap();

        let accepted = journal.list_by_state(JournalState::Accepted).await.unwrap();
        let committed = journal
            .list_by_state(JournalState::Committed)
            .await
            .unwrap();

        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0].command_id, accepted_id);
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].command_id, committed_id);
    }

    #[tokio::test]
    async fn next_sequence_for_origin_advances_from_durable_journal() {
        let temp_dir = TempDir::new().unwrap();
        let journal = SqliteConsensusJournal::new(temp_dir.path()).await.unwrap();
        let first_id = ConsensusCommandId::new(ORIGIN_NODE_ID.to_owned(), 1);
        let third_id = ConsensusCommandId::new(ORIGIN_NODE_ID.to_owned(), 3);
        let other_id = ConsensusCommandId::new("node-b".to_owned(), 9);

        let empty = journal
            .next_sequence_for_origin(ORIGIN_NODE_ID)
            .await
            .unwrap();
        let _ = journal
            .record_committed(&first_id, COMMAND_BYTES)
            .await
            .unwrap();
        let _ = journal
            .record_committed(&third_id, COMMAND_BYTES)
            .await
            .unwrap();
        let _ = journal
            .record_committed(&other_id, COMMAND_BYTES)
            .await
            .unwrap();
        let next = journal
            .next_sequence_for_origin(ORIGIN_NODE_ID)
            .await
            .unwrap();

        assert_eq!(empty, 1);
        assert_eq!(next, 4);
    }

    #[tokio::test]
    async fn applied_state_is_not_downgraded_by_older_transition() {
        let temp_dir = TempDir::new().unwrap();
        let journal = SqliteConsensusJournal::new(temp_dir.path()).await.unwrap();

        let _ = journal
            .record_applied(&command_id(), COMMAND_BYTES, RESULT_BYTES)
            .await
            .unwrap();
        let entry = journal
            .record_committed(&command_id(), COMMAND_BYTES)
            .await
            .unwrap();

        assert_eq!(entry.state, JournalState::Applied);
        assert_eq!(entry.result, RESULT_BYTES.to_vec());
    }

    #[tokio::test]
    async fn rejects_conflicting_payload_for_existing_command_id() {
        let temp_dir = TempDir::new().unwrap();
        let journal = SqliteConsensusJournal::new(temp_dir.path()).await.unwrap();

        let _ = journal
            .record_pre_accepted(&command_id(), COMMAND_BYTES)
            .await
            .unwrap();
        let error = journal
            .record_accepted(&command_id(), b"different-command")
            .await
            .unwrap_err();

        assert!(matches!(error, So3Error::InvalidRequest(_)));
        assert!(error.to_string().contains("conflicting command payload"));
    }

    #[tokio::test]
    async fn open_rejects_unknown_schema_version() {
        let temp_dir = TempDir::new().unwrap();
        let journal = SqliteConsensusJournal::new(temp_dir.path()).await.unwrap();
        journal
            .set_schema_version_for_test(UNKNOWN_SCHEMA_VERSION)
            .await
            .unwrap();
        drop(journal);

        let error = SqliteConsensusJournal::new(temp_dir.path())
            .await
            .unwrap_err();

        assert!(matches!(error, So3Error::Storage(_)));
        assert!(
            error
                .to_string()
                .contains("unsupported consensus sqlite schema version")
        );
    }
}
