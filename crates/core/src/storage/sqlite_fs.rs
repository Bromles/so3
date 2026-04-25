use std::fmt::Write as FmtWrite;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use sqlx::{Row, SqlitePool, query, query_scalar};
use tokio::fs;
use tokio::fs::File as TokioFile;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::domain::error::{So3Error, So3Result};
use crate::domain::{ObjectKey, ObjectRecord, ObjectVersion, StoredObject};
use crate::storage::repository::{CasWriteOutcome, ObjectRepository};

// SQLite runtime tuning.
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_MAX_CONNECTIONS: u32 = 1;

// Metadata schema versioning.
const CURRENT_SCHEMA_VERSION: i64 = 1;

// On-disk layout.
const METADATA_DIR_NAME: &str = "metadata";
const BLOBS_DIR_NAME: &str = "blobs";
const TEMP_BLOBS_DIR_NAME: &str = "tmp";
const COMMITTED_BLOBS_DIR_NAME: &str = "committed";
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
const LOAD_OBJECT_SQL: &str = r"
    SELECT key, version, blob_id, content_length, checksum
    FROM objects
    WHERE key = ?
";
const UPSERT_OBJECT_SQL: &str = r"
    INSERT INTO objects (key, version, blob_id, content_length, checksum)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET
        version = excluded.version,
        blob_id = excluded.blob_id,
        content_length = excluded.content_length,
        checksum = excluded.checksum
";

pub struct PersistentObjectStore {
    pool: SqlitePool,
    blob_dir: PathBuf,
    write_lock: Mutex<()>,
}

impl PersistentObjectStore {
    /// # Errors
    ///
    /// Returns an error if the local metadata database or blob directories cannot be created or opened.
    pub async fn new(data_dir: impl AsRef<Path>) -> So3Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let metadata_dir = data_dir.join(METADATA_DIR_NAME);
        let blob_dir = data_dir.join(BLOBS_DIR_NAME);
        fs::create_dir_all(&metadata_dir).await?;
        fs::create_dir_all(blob_dir.join(TEMP_BLOBS_DIR_NAME)).await?;
        fs::create_dir_all(blob_dir.join(COMMITTED_BLOBS_DIR_NAME)).await?;

        let database_path = metadata_dir.join(DATABASE_FILE_NAME);
        let options = SqliteConnectOptions::new()
            .filename(database_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(SQLITE_BUSY_TIMEOUT);

        let pool = SqlitePoolOptions::new()
            .max_connections(SQLITE_MAX_CONNECTIONS)
            .connect_with(options)
            .await?;

        let store = Self {
            pool,
            blob_dir,
            write_lock: Mutex::new(()),
        };

        store.init_schema().await?;
        Ok(store)
    }

    async fn init_schema(&self) -> So3Result<()> {
        let version = query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&self.pool)
            .await?;

        match version {
            0 => self.migrate_to_v1().await,
            CURRENT_SCHEMA_VERSION => Ok(()),
            unsupported => Err(So3Error::Storage(format!(
                "unsupported sqlite schema version: {unsupported}"
            ))),
        }
    }

    async fn migrate_to_v1(&self) -> So3Result<()> {
        query(OBJECTS_TABLE_SQL).execute(&self.pool).await?;
        query(&format!("PRAGMA user_version = {CURRENT_SCHEMA_VERSION}"))
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn persist_blob(&self, value: &[u8]) -> So3Result<(String, u64, String)> {
        let blob_id = format!("{}.blob", Uuid::new_v4());
        let temp_path = self
            .blob_dir
            .join(TEMP_BLOBS_DIR_NAME)
            .join(format!("{blob_id}.tmp"));
        let final_path = self.blob_dir.join(COMMITTED_BLOBS_DIR_NAME).join(&blob_id);

        let mut file = TokioFile::create(&temp_path).await?;
        file.write_all(value).await?;
        file.sync_all().await?;
        drop(file);

        fs::rename(&temp_path, &final_path).await?;

        let checksum = checksum_hex(value);
        Ok((blob_id, value.len() as u64, checksum))
    }

    async fn load_blob(&self, blob_id: &str) -> So3Result<Vec<u8>> {
        let path = self.blob_dir.join(COMMITTED_BLOBS_DIR_NAME).join(blob_id);
        fs::read(path).await.map_err(So3Error::from)
    }

    async fn load_record(&self, key: &ObjectKey) -> So3Result<Option<ObjectRecord>> {
        let row = query(LOAD_OBJECT_SQL)
            .bind(key.as_str())
            .fetch_optional(&self.pool)
            .await?;

        row.as_ref().map(Self::row_to_record).transpose()
    }

    fn row_to_record(row: &SqliteRow) -> So3Result<ObjectRecord> {
        Ok(ObjectRecord {
            key: ObjectKey::new(row.try_get::<String, _>("key")?)?,
            version: ObjectVersion::try_from(row.try_get::<i64, _>("version")?)?,
            blob_id: row.try_get("blob_id")?,
            content_length: read_content_length(row)?,
            checksum: row.try_get("checksum")?,
        })
    }

    async fn write_record(
        &self,
        key: &ObjectKey,
        version: ObjectVersion,
        value: Vec<u8>,
    ) -> So3Result<StoredObject> {
        let (blob_id, content_length, checksum) = self.persist_blob(&value).await?;

        query(UPSERT_OBJECT_SQL)
            .bind(key.as_str())
            .bind(version.get())
            .bind(&blob_id)
            .bind(content_length_to_i64(content_length)?)
            .bind(&checksum)
            .execute(&self.pool)
            .await?;

        Ok(StoredObject {
            record: ObjectRecord {
                key: key.clone(),
                version,
                blob_id,
                content_length,
                checksum,
            },
            value,
        })
    }
}

#[async_trait]
impl ObjectRepository for PersistentObjectStore {
    async fn read(&self, key: &ObjectKey) -> So3Result<Option<StoredObject>> {
        let Some(record) = self.load_record(key).await? else {
            return Ok(None);
        };

        let value = self.load_blob(&record.blob_id).await?;
        Ok(Some(StoredObject { record, value }))
    }

    async fn write(&self, key: &ObjectKey, value: Vec<u8>) -> So3Result<StoredObject> {
        let _guard = self.write_lock.lock().await;
        let next_version = self
            .load_record(key)
            .await?
            .map_or_else(ObjectVersion::initial, |record| record.version.next());

        self.write_record(key, next_version, value).await
    }

    async fn cas(
        &self,
        key: &ObjectKey,
        expected_version: ObjectVersion,
        value: Vec<u8>,
    ) -> So3Result<CasWriteOutcome> {
        let _guard = self.write_lock.lock().await;
        let Some(current) = self.load_record(key).await? else {
            return Ok(CasWriteOutcome::NotFound);
        };

        if current.version != expected_version {
            return Ok(CasWriteOutcome::Mismatch {
                current_version: current.version,
            });
        }

        let object = self
            .write_record(key, current.version.next(), value)
            .await?;
        Ok(CasWriteOutcome::Applied(object))
    }
}

fn checksum_hex(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let mut checksum = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = FmtWrite::write_fmt(&mut checksum, format_args!("{byte:02x}"));
    }
    checksum
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
    use sqlx::{query, query_scalar};
    use tempfile::TempDir;

    use super::PersistentObjectStore;
    use crate::domain::error::So3Error;
    use crate::domain::{ObjectKey, ObjectVersion};
    use crate::storage::repository::{CasWriteOutcome, ObjectRepository};

    const UNKNOWN_SCHEMA_VERSION: i64 = 99;
    const FIRST_PAYLOAD: &[u8] = b"first";
    const SECOND_PAYLOAD: &[u8] = b"second";
    const HELLO_PAYLOAD: &[u8] = b"hello";
    const INITIAL_VERSION_NUMBER: i64 = 1;
    const STALE_VERSION_NUMBER: i64 = 99;
    const KEY_ALPHA: &str = "alpha";
    const KEY_BETA: &str = "beta";
    const KEY_GAMMA: &str = "gamma";

    #[tokio::test]
    async fn write_survives_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let key = ObjectKey::new(KEY_ALPHA).unwrap();

        let store = PersistentObjectStore::new(temp_dir.path()).await.unwrap();
        let written = store.write(&key, HELLO_PAYLOAD.to_vec()).await.unwrap();
        assert_eq!(written.record.version, ObjectVersion::initial());
        drop(store);

        let reopened = PersistentObjectStore::new(temp_dir.path()).await.unwrap();
        let loaded = reopened.read(&key).await.unwrap().unwrap();

        assert_eq!(loaded.record.version, ObjectVersion::initial());
        assert_eq!(loaded.value, HELLO_PAYLOAD.to_vec());
    }

    #[tokio::test]
    async fn cas_reports_mismatch_without_overwriting() {
        let temp_dir = TempDir::new().unwrap();
        let key = ObjectKey::new(KEY_BETA).unwrap();
        let store = PersistentObjectStore::new(temp_dir.path()).await.unwrap();

        let written = store.write(&key, FIRST_PAYLOAD.to_vec()).await.unwrap();
        let outcome = store
            .cas(
                &key,
                ObjectVersion::try_from(STALE_VERSION_NUMBER).unwrap(),
                SECOND_PAYLOAD.to_vec(),
            )
            .await
            .unwrap();

        assert_eq!(
            outcome,
            CasWriteOutcome::Mismatch {
                current_version: written.record.version,
            }
        );

        let loaded = store.read(&key).await.unwrap().unwrap();
        assert_eq!(loaded.value, FIRST_PAYLOAD.to_vec());
    }

    #[tokio::test]
    async fn open_sets_expected_schema_version() {
        let temp_dir = TempDir::new().unwrap();
        let store = PersistentObjectStore::new(temp_dir.path()).await.unwrap();

        let version = query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&store.pool)
            .await
            .unwrap();

        assert_eq!(version, INITIAL_VERSION_NUMBER);
    }

    #[tokio::test]
    async fn open_rejects_unknown_schema_version() {
        let temp_dir = TempDir::new().unwrap();
        let store = PersistentObjectStore::new(temp_dir.path()).await.unwrap();
        query(&format!("PRAGMA user_version = {UNKNOWN_SCHEMA_VERSION}"))
            .execute(&store.pool)
            .await
            .unwrap();
        drop(store);

        let Err(error) = PersistentObjectStore::new(temp_dir.path()).await else {
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
    async fn cas_applies_new_value_and_bumps_version() {
        let temp_dir = TempDir::new().unwrap();
        let key = ObjectKey::new(KEY_GAMMA).unwrap();
        let store = PersistentObjectStore::new(temp_dir.path()).await.unwrap();

        let written = store.write(&key, FIRST_PAYLOAD.to_vec()).await.unwrap();
        let outcome = store
            .cas(&key, written.record.version, SECOND_PAYLOAD.to_vec())
            .await
            .unwrap();

        let CasWriteOutcome::Applied(object) = outcome else {
            panic!("expected applied cas outcome");
        };

        assert_eq!(object.record.version, written.record.version.next());
        assert_eq!(object.value, SECOND_PAYLOAD.to_vec());
    }
}
