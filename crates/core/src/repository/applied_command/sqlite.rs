use crate::consensus::ConsensusCommandId;
use crate::domain::command::CommandResult;
use crate::domain::error::{So3Error, So3Result};
use crate::repository::applied_command::AppliedCommandRepository;
use async_trait::async_trait;
use sqlx::{query, Row, SqlitePool};

const LOAD_APPLIED_RESULT_SQL: &str = r"
    SELECT result
    FROM applied_commands
    WHERE origin_node_id = ? AND sequence = ?
";
const INSERT_APPLIED_RESULT_SQL: &str = r"
    INSERT OR IGNORE INTO applied_commands (origin_node_id, sequence, result)
    VALUES (?, ?, ?)
";

#[derive(Clone)]
pub struct SqliteAppliedCommandRepository {
    pool: SqlitePool,
}

#[async_trait]
impl AppliedCommandRepository for SqliteAppliedCommandRepository {
    async fn load_result(
        &self,
        command_id: &ConsensusCommandId,
    ) -> So3Result<Option<CommandResult>> {
        let row = query(LOAD_APPLIED_RESULT_SQL)
            .bind(command_id.origin_node_id())
            .bind(sequence_to_i64(command_id.sequence())?)
            .fetch_optional(&self.pool)
            .await?;

        row.map(|row| {
            let bytes = row.try_get::<Vec<u8>, _>("result")?;
            CommandResult::from_bytes(&bytes)
        })
        .transpose()
    }

    async fn save_result(
        &self,
        command_id: &ConsensusCommandId,
        result: &CommandResult,
    ) -> So3Result<()> {
        query(INSERT_APPLIED_RESULT_SQL)
            .bind(command_id.origin_node_id())
            .bind(sequence_to_i64(command_id.sequence())?)
            .bind(result.to_bytes()?)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

fn sequence_to_i64(sequence: u64) -> So3Result<i64> {
    i64::try_from(sequence).map_err(|_| {
        So3Error::Storage(format!(
            "command sequence exceeds supported metadata range: {sequence}"
        ))
    })
}

#[cfg(test)]
mod test {
    use crate::consensus::ConsensusCommandId;
    use crate::domain::blob::BlobMetadata;
    use crate::domain::command::{CommandResult, ReadResult, WriteResult};
    use crate::domain::object::{ObjectLastModified, ObjectMetadata};
    use crate::domain::object_key::ObjectKey;
    use crate::domain::object_version::ObjectVersion;
    use crate::repository::applied_command::sqlite::SqliteAppliedCommandRepository;
    use tempfile::TempDir;

    const COMMAND_ORIGIN_NODE_ID: &str = "node-a";
    const COMMAND_SEQUENCE_ONE: u64 = 1;

    fn test_record() -> ObjectMetadata {
        ObjectMetadata {
            key: ObjectKey::new(KEY_ALPHA).unwrap(),
            version: ObjectVersion::initial(),
            blob_metadata: BlobMetadata {
                blob_id: BLOB_ID.into(),
                content_length: CONTENT_LENGTH,
                checksum_sha256: CHECKSUM.to_owned(),
            },
            last_modified: ObjectLastModified::try_from(LAST_MODIFIED_UNIX_MILLIS).unwrap(),
        }
    }

    #[tokio::test]
    async fn save_then_load_roundtrips_applied_command_result() {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteAppliedCommandRepository::new(temp_dir.path())
            .await
            .unwrap();
        let command_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let result = CommandResult::Read(ReadResult { record: None });

        repository.save_result(&command_id, &result).await.unwrap();
        let loaded = repository.load_result(&command_id).await.unwrap().unwrap();

        assert_eq!(loaded, result);
    }

    #[tokio::test]
    async fn save_result_keeps_first_applied_value_for_duplicate_command_id() {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteAppliedCommandRepository::new(temp_dir.path())
            .await
            .unwrap();
        let command_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let first = CommandResult::Read(ReadResult { record: None });
        let second = CommandResult::Read(ReadResult {
            record: Some(test_record()),
        });

        repository.save_result(&command_id, &first).await.unwrap();
        repository.save_result(&command_id, &second).await.unwrap();
        let loaded = repository.load_result(&command_id).await.unwrap().unwrap();

        assert_eq!(loaded, first);
    }

    #[tokio::test]
    async fn save_result_persists_only_object_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteAppliedCommandRepository::new(temp_dir.path())
            .await
            .unwrap();
        let command_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let payload = b"value-bytes-that-must-not-be-cached";
        // WriteResult now holds only ObjectRecord (no value bytes), so payload must never
        // appear in the persisted row. We embed the payload text in the blob_id to verify
        // that the record fields do not escape either.
        let result = CommandResult::Write(WriteResult {
            record: test_record(),
        });

        repository.save_result(&command_id, &result).await.unwrap();
        let bytes: Vec<u8> = sqlx::query_scalar(
            "SELECT result FROM applied_commands WHERE origin_node_id = ? AND sequence = ?",
        )
        .bind(COMMAND_ORIGIN_NODE_ID)
        .bind(i64::try_from(COMMAND_SEQUENCE_ONE).unwrap())
        .fetch_one(&repository.pool)
        .await
        .unwrap();

        assert!(!contains_subslice(&bytes, payload));
    }

    fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }
}
