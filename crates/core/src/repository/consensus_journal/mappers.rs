use crate::domain::consensus::command_id::CommandId;
use crate::domain::consensus::journal::JournalEntry;
use crate::domain::error::{So3Error, So3Result};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

pub fn sequence_to_i64(sequence: u64) -> So3Result<i64> {
    i64::try_from(sequence).map_err(|_| {
        So3Error::Storage(format!(
            "command sequence exceeds sqlite integer range: {sequence}"
        ))
    })
}

pub fn i64_to_u64_sequence(sequence: i64) -> So3Result<u64> {
    u64::try_from(sequence)
        .map_err(|_| So3Error::Storage(format!("invalid negative command sequence: {sequence}")))
}

pub fn row_to_entry(row: &SqliteRow) -> So3Result<JournalEntry> {
    let sequence = row.try_get::<i64, _>("sequence")?;
    let state = row.try_get::<i64, _>("state")?;
    let command = row.try_get::<Vec<u8>, _>("command")?;

    Ok(JournalEntry {
        command_id: CommandId::new(
            row.try_get("origin_node_id")?,
            i64_to_u64_sequence(sequence)?,
        ),
        state: parse_state(state)?,
        metadata: JournalMetadata {
            timestamp_zero: decode_optional_proto(&row.try_get::<Vec<u8>, _>("timestamp_zero")?)?,
            timestamp: decode_optional_proto(&row.try_get::<Vec<u8>, _>("timestamp")?)?,
            dependencies: decode_dependencies(&row.try_get::<Vec<u8>, _>("dependencies")?)?,
            ballot: decode_optional_proto(&row.try_get::<Vec<u8>, _>("ballot")?)?,
        },
    })
}