use crate::domain::clock::LogicalTimestamp;
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::consensus::ballot::Ballot;
use crate::domain::consensus::command_id::{CommandId, DependencySet};
use crate::domain::consensus::journal::{JournalEntry, JournalState};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::node::NodeId;
use sqlx::Row;
use sqlx::sqlite::SqliteRow;

pub fn sequence_to_i64(sequence: u64) -> So3Result<i64> {
    i64::try_from(sequence)
        .map_err(|_| So3Error::Storage(format!("sequence {sequence} exceeds i64 range")))
}

pub fn i64_to_u64(value: i64, field: &str) -> So3Result<u64> {
    u64::try_from(value).map_err(|_| So3Error::Storage(format!("negative {field} in db: {value}")))
}

pub fn encode_command(command: &ObjectCommand) -> So3Result<Vec<u8>> {
    postcard::to_allocvec(command)
        .map_err(|e| So3Error::Serialization(format!("command encode: {e}")))
}

pub fn decode_command(bytes: &[u8]) -> So3Result<ObjectCommand> {
    postcard::from_bytes(bytes).map_err(|e| So3Error::Serialization(format!("command decode: {e}")))
}

pub fn encode_deps(deps: &[CommandId]) -> So3Result<Vec<u8>> {
    postcard::to_allocvec(deps).map_err(|e| So3Error::Serialization(format!("deps encode: {e}")))
}

pub fn decode_deps(bytes: &[u8]) -> So3Result<Vec<CommandId>> {
    postcard::from_bytes(bytes).map_err(|e| So3Error::Serialization(format!("deps decode: {e}")))
}

pub fn encode_result(result: &CommandResult) -> So3Result<Vec<u8>> {
    postcard::to_allocvec(result)
        .map_err(|e| So3Error::Serialization(format!("result encode: {e}")))
}

pub fn decode_result(bytes: &[u8]) -> So3Result<CommandResult> {
    postcard::from_bytes(bytes).map_err(|e| So3Error::Serialization(format!("result decode: {e}")))
}

pub fn command_key(command: &ObjectCommand) -> &str {
    match command {
        ObjectCommand::Read { key }
        | ObjectCommand::Write { key, .. }
        | ObjectCommand::Cas { key, .. }
        | ObjectCommand::Delete { key } => key.as_ref(),
    }
}

/// Reconstructs a `JournalEntry` from a single journal row.
pub fn row_to_entry(row: &SqliteRow) -> So3Result<JournalEntry> {
    let deps_bytes: Vec<u8> = row.try_get("deps")?;
    let deps = decode_deps(&deps_bytes)?;
    let origin_node_id: String = row.try_get("origin_node_id")?;
    let sequence: i64 = row.try_get("sequence")?;
    let state_raw: i32 = row.try_get("state")?;
    let command_bytes: Vec<u8> = row.try_get("command")?;

    let command_id = CommandId {
        origin_node_id: NodeId::new(origin_node_id),
        sequence: i64_to_u64(sequence, "sequence")?,
    };
    let state = JournalState::try_from(state_raw)?;
    let command = decode_command(&command_bytes)?;

    let timestamp_zero = row_to_timestamp(row, "t0")?;

    let timestamp = if row.try_get::<Option<i64>, _>("t_epoch")?.is_some() {
        Some(row_to_timestamp(row, "t")?)
    } else {
        None
    };

    let ballot = if row.try_get::<Option<i64>, _>("ballot_round")?.is_some() {
        let round = i64_to_u64(row.try_get::<i64, _>("ballot_round")?, "ballot_round")?;
        let node_id = NodeId::new(row.try_get::<String, _>("ballot_node_id")?);
        Some(Ballot { round, node_id })
    } else {
        None
    };

    let result = row
        .try_get::<Option<Vec<u8>>, _>("result")?
        .map(|b| decode_result(&b))
        .transpose()?;

    Ok(JournalEntry {
        command_id,
        command,
        state,
        timestamp_zero,
        timestamp,
        dependencies: DependencySet(deps),
        ballot,
        result,
    })
}

fn row_to_timestamp(row: &SqliteRow, prefix: &str) -> So3Result<LogicalTimestamp> {
    let epoch = i64_to_u64(
        row.try_get::<i64, _>(format!("{prefix}_epoch").as_str())?,
        "epoch",
    )?;
    let physical_millis = i64_to_u64(
        row.try_get::<i64, _>(format!("{prefix}_physical_ms").as_str())?,
        "physical_ms",
    )?;
    let logical = i64_to_u64(
        row.try_get::<i64, _>(format!("{prefix}_logical").as_str())?,
        "logical",
    )?;
    let node_id = NodeId::new(row.try_get::<String, _>(format!("{prefix}_node_id").as_str())?);
    Ok(LogicalTimestamp {
        epoch,
        physical_millis,
        logical,
        node_id,
    })
}
