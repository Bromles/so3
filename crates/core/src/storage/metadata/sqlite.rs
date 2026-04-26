use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use sqlx::{Row, SqlitePool, query, query_scalar};
use tokio::fs;

use crate::consensus::ConsensusCommandId;
use crate::domain::error::{So3Error, So3Result};
use crate::domain::{ObjectKey, ObjectLastModified, ObjectRecord, ObjectResult, ObjectVersion};
use crate::storage::applied_command::repository::AppliedCommandStore;
use crate::storage::metadata::repository::ObjectMetadataRepository;

// SQLite runtime tuning.
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_MAX_CONNECTIONS: u32 = 1;

// Metadata schema versioning.
const CURRENT_SCHEMA_VERSION: i64 = 3;
const APPLIED_COMMANDS_SCHEMA_VERSION: i64 = 2;

// On-disk metadata layout.
const DATABASE_FILE_NAME: &str = "objects.sqlite";

// Metadata SQL.
const OBJECTS_TABLE_SQL: &str = r"
    CREATE TABLE IF NOT EXISTS objects (
        key TEXT PRIMARY KEY,
        version INTEGER NOT NULL,
        blob_id TEXT NOT NULL,
        content_length INTEGER NOT NULL,
        checksum TEXT NOT NULL
    )
";
const APPLIED_COMMANDS_TABLE_SQL: &str = r"
    CREATE TABLE IF NOT EXISTS applied_commands (
        origin_node_id TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        result BLOB NOT NULL,
        PRIMARY KEY (origin_node_id, sequence)
    )
";
const LOAD_OBJECT_SQL: &str = r"
    SELECT key, version, blob_id, content_length, checksum, last_modified_unix_millis
    FROM objects
    WHERE key = ?
";
const LOAD_APPLIED_RESULT_SQL: &str = r"
    SELECT result
    FROM applied_commands
    WHERE origin_node_id = ? AND sequence = ?
";
const UPSERT_OBJECT_SQL: &str = r"
    INSERT INTO objects (key, version, blob_id, content_length, checksum, last_modified_unix_millis)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET
        version = excluded.version,
        blob_id = excluded.blob_id,
        content_length = excluded.content_length,
        checksum = excluded.checksum,
        last_modified_unix_millis = excluded.last_modified_unix_millis
";
const INSERT_APPLIED_RESULT_SQL: &str = r"
    INSERT OR IGNORE INTO applied_commands (origin_node_id, sequence, result)
    VALUES (?, ?, ?)
";

#[derive(Clone)]
pub struct SqliteObjectMetadataRepository {
    pool: SqlitePool,
}

impl SqliteObjectMetadataRepository {
    /// # Errors
    ///
    /// Returns an error if the local metadata database cannot be created, opened, or migrated.
    pub async fn new(metadata_dir: impl AsRef<Path>) -> So3Result<Self> {
        let metadata_dir = metadata_dir.as_ref().to_path_buf();
        fs::create_dir_all(&metadata_dir).await?;

        let options = SqliteConnectOptions::new()
            .filename(metadata_dir.join(DATABASE_FILE_NAME))
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(SQLITE_BUSY_TIMEOUT);

        let pool = SqlitePoolOptions::new()
            .max_connections(SQLITE_MAX_CONNECTIONS)
            .connect_with(options)
            .await?;

        let repository = Self { pool };
        repository.init_schema().await?;
        Ok(repository)
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
                "unsupported sqlite schema version: {unsupported}"
            ))),
        }
    }

    async fn migrate_to_v1(&self) -> So3Result<()> {
        query(OBJECTS_TABLE_SQL).execute(&self.pool).await?;
        query("PRAGMA user_version = 1").execute(&self.pool).await?;
        Ok(())
    }

    async fn migrate_to_v2(&self) -> So3Result<()> {
        query(APPLIED_COMMANDS_TABLE_SQL)
            .execute(&self.pool)
            .await?;
        query(&format!(
            "PRAGMA user_version = {APPLIED_COMMANDS_SCHEMA_VERSION}"
        ))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn migrate_to_v3(&self) -> So3Result<()> {
        query(
            "ALTER TABLE objects ADD COLUMN last_modified_unix_millis INTEGER NOT NULL DEFAULT 0",
        )
        .execute(&self.pool)
        .await?;
        query(
            "UPDATE objects SET last_modified_unix_millis = ? WHERE last_modified_unix_millis = 0",
        )
        .bind(ObjectLastModified::now()?.unix_millis())
        .execute(&self.pool)
        .await?;
        query(&format!("PRAGMA user_version = {CURRENT_SCHEMA_VERSION}"))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    fn row_to_record(row: &SqliteRow) -> So3Result<ObjectRecord> {
        Ok(ObjectRecord {
            key: ObjectKey::new(row.try_get::<String, _>("key")?)?,
            version: ObjectVersion::try_from(row.try_get::<i64, _>("version")?)?,
            blob_id: row.try_get("blob_id")?,
            content_length: read_content_length(row)?,
            checksum: row.try_get("checksum")?,
            last_modified: ObjectLastModified::try_from(
                row.try_get::<i64, _>("last_modified_unix_millis")?,
            )?,
        })
    }

    #[cfg(test)]
    async fn schema_version(&self) -> So3Result<i64> {
        query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&self.pool)
            .await
            .map_err(So3Error::from)
    }

    #[cfg(test)]
    async fn set_schema_version_for_test(&self, version: i64) -> So3Result<()> {
        query(&format!("PRAGMA user_version = {version}"))
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl ObjectMetadataRepository for SqliteObjectMetadataRepository {
    async fn read(&self, key: &ObjectKey) -> So3Result<Option<ObjectRecord>> {
        let row = query(LOAD_OBJECT_SQL)
            .bind(key.as_str())
            .fetch_optional(&self.pool)
            .await?;

        row.as_ref().map(Self::row_to_record).transpose()
    }

    async fn write(&self, record: &ObjectRecord) -> So3Result<()> {
        query(UPSERT_OBJECT_SQL)
            .bind(record.key.as_str())
            .bind(record.version.get())
            .bind(&record.blob_id)
            .bind(content_length_to_i64(record.content_length)?)
            .bind(&record.checksum)
            .bind(record.last_modified.unix_millis())
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl AppliedCommandStore for SqliteObjectMetadataRepository {
    async fn load_result(
        &self,
        command_id: &ConsensusCommandId,
    ) -> So3Result<Option<ObjectResult>> {
        let row = query(LOAD_APPLIED_RESULT_SQL)
            .bind(command_id.origin_node_id())
            .bind(sequence_to_i64(command_id.sequence())?)
            .fetch_optional(&self.pool)
            .await?;

        row.map(|row| {
            let bytes = row.try_get::<Vec<u8>, _>("result")?;
            ObjectResult::from_bytes(&bytes)
        })
        .transpose()
    }

    async fn save_result(
        &self,
        command_id: &ConsensusCommandId,
        result: &ObjectResult,
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

fn read_content_length(row: &SqliteRow) -> So3Result<u64> {
    let content_length = row.try_get::<i64, _>("content_length")?;
    u64::try_from(content_length).map_err(|_| {
        So3Error::Storage(format!(
            "invalid negative content_length in metadata: {content_length}"
        ))
    })
}

fn content_length_to_i64(content_length: u64) -> So3Result<i64> {
    i64::try_from(content_length).map_err(|_| {
        So3Error::Storage(format!(
            "content_length exceeds supported metadata range: {content_length}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::SqliteObjectMetadataRepository;
    use crate::consensus::ConsensusCommandId;
    use crate::domain::error::So3Error;
    use crate::domain::{
        ObjectKey, ObjectLastModified, ObjectRecord, ObjectResult, ObjectVersion, ReadResult,
    };
    use crate::storage::applied_command::repository::AppliedCommandStore;
    use crate::storage::metadata::repository::ObjectMetadataRepository;

    const UNKNOWN_SCHEMA_VERSION: i64 = 99;
    const KEY_ALPHA: &str = "alpha";
    const BLOB_ID: &str = "blob-1.blob";
    const CHECKSUM: &str = "checksum";
    const CONTENT_LENGTH: u64 = 5;
    const LAST_MODIFIED_UNIX_MILLIS: i64 = 1_775_000_000_123;
    const COMMAND_ORIGIN_NODE_ID: &str = "node-a";
    const COMMAND_SEQUENCE_ONE: u64 = 1;

    fn test_record() -> ObjectRecord {
        ObjectRecord {
            key: ObjectKey::new(KEY_ALPHA).unwrap(),
            version: ObjectVersion::initial(),
            blob_id: BLOB_ID.to_owned(),
            content_length: CONTENT_LENGTH,
            checksum: CHECKSUM.to_owned(),
            last_modified: ObjectLastModified::try_from(LAST_MODIFIED_UNIX_MILLIS).unwrap(),
        }
    }

    #[tokio::test]
    async fn write_then_read_roundtrips_record() {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteObjectMetadataRepository::new(temp_dir.path())
            .await
            .unwrap();
        let record = test_record();

        repository.write(&record).await.unwrap();
        let loaded = repository.read(&record.key).await.unwrap().unwrap();

        assert_eq!(loaded, record);
    }

    #[tokio::test]
    async fn open_sets_expected_schema_version() {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteObjectMetadataRepository::new(temp_dir.path())
            .await
            .unwrap();

        assert_eq!(
            repository.schema_version().await.unwrap(),
            super::CURRENT_SCHEMA_VERSION
        );
    }

    #[tokio::test]
    async fn open_rejects_unknown_schema_version() {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteObjectMetadataRepository::new(temp_dir.path())
            .await
            .unwrap();
        repository
            .set_schema_version_for_test(UNKNOWN_SCHEMA_VERSION)
            .await
            .unwrap();
        drop(repository);

        let Err(error) = SqliteObjectMetadataRepository::new(temp_dir.path()).await else {
            panic!("expected unsupported schema version error");
        };

        assert!(matches!(error, So3Error::Storage(_)));
        assert!(
            error
                .to_string()
                .contains("unsupported sqlite schema version")
        );
    }

    #[tokio::test]
    async fn save_then_load_roundtrips_applied_command_result() {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteObjectMetadataRepository::new(temp_dir.path())
            .await
            .unwrap();
        let command_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let result = ObjectResult::Read(ReadResult { object: None });

        repository.save_result(&command_id, &result).await.unwrap();
        let loaded = repository.load_result(&command_id).await.unwrap().unwrap();

        assert_eq!(loaded, result);
    }

    #[tokio::test]
    async fn save_result_keeps_first_applied_value_for_duplicate_command_id() {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteObjectMetadataRepository::new(temp_dir.path())
            .await
            .unwrap();
        let command_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let first = ObjectResult::Read(ReadResult { object: None });
        let second = ObjectResult::Read(ReadResult {
            object: Some(crate::domain::StoredObject {
                record: test_record(),
                value: b"second".to_vec(),
            }),
        });

        repository.save_result(&command_id, &first).await.unwrap();
        repository.save_result(&command_id, &second).await.unwrap();
        let loaded = repository.load_result(&command_id).await.unwrap().unwrap();

        assert_eq!(loaded, first);
    }
}
