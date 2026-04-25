use std::fmt::Write as FmtWrite;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::query;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use sqlx::{Row, SqlitePool};
use tokio::fs;
use tokio::fs::File as TokioFile;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::domain::error::{So3Error, So3Result};
use crate::domain::types::{ObjectKey, ObjectRecord, ObjectVersion, StoredObject};
use crate::storage::repository::{CasWriteOutcome, ObjectRepository};

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_MAX_CONNECTIONS: u32 = 1;

pub struct PersistentObjectStore {
    pool: SqlitePool,
    blob_dir: PathBuf,
    write_lock: Mutex<()>,
}

impl PersistentObjectStore {
    pub async fn open(data_dir: impl AsRef<Path>) -> So3Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let metadata_dir = data_dir.join("metadata");
        let blob_dir = data_dir.join("blobs");
        fs::create_dir_all(&metadata_dir).await?;
        fs::create_dir_all(blob_dir.join("tmp")).await?;
        fs::create_dir_all(blob_dir.join("committed")).await?;

        let database_path = metadata_dir.join("objects.sqlite");
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
        query(
            r#"
            CREATE TABLE IF NOT EXISTS objects (
                key TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                blob_id TEXT NOT NULL,
                content_length INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn persist_blob(&self, value: &[u8]) -> So3Result<(String, u64, String)> {
        let blob_id = format!("{}.blob", Uuid::new_v4());
        let temp_path = self.blob_dir.join("tmp").join(format!("{blob_id}.tmp"));
        let final_path = self.blob_dir.join("committed").join(&blob_id);

        let mut file = TokioFile::create(&temp_path).await?;
        file.write_all(value).await?;
        file.sync_all().await?;
        drop(file);

        fs::rename(&temp_path, &final_path).await?;

        let checksum = checksum_hex(value);
        Ok((blob_id, value.len() as u64, checksum))
    }

    async fn load_blob(&self, blob_id: &str) -> So3Result<Vec<u8>> {
        let path = self.blob_dir.join("committed").join(blob_id);
        fs::read(path).await.map_err(So3Error::from)
    }

    async fn load_record(&self, key: &ObjectKey) -> So3Result<Option<ObjectRecord>> {
        let row = query(
            r#"
            SELECT key, version, blob_id, content_length, checksum, updated_at_unix_ms
            FROM objects
            WHERE key = ?
            "#,
        )
        .bind(key.as_str())
        .fetch_optional(&self.pool)
        .await?;

        row.map(Self::row_to_record).transpose()
    }

    fn row_to_record(row: SqliteRow) -> So3Result<ObjectRecord> {
        Ok(ObjectRecord {
            key: ObjectKey::new(row.try_get::<String, _>("key")?)?,
            version: ObjectVersion::try_from(row.try_get::<i64, _>("version")?)?,
            blob_id: row.try_get("blob_id")?,
            content_length: row.try_get::<i64, _>("content_length")? as u64,
            checksum: row.try_get("checksum")?,
            updated_at_unix_ms: row.try_get::<i64, _>("updated_at_unix_ms")? as u64,
        })
    }

    async fn write_record(
        &self,
        key: &ObjectKey,
        version: ObjectVersion,
        value: Vec<u8>,
    ) -> So3Result<StoredObject> {
        let (blob_id, content_length, checksum) = self.persist_blob(&value).await?;
        let updated_at_unix_ms = unix_time_ms();

        query(
            r#"
            INSERT INTO objects (key, version, blob_id, content_length, checksum, updated_at_unix_ms)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                version = excluded.version,
                blob_id = excluded.blob_id,
                content_length = excluded.content_length,
                checksum = excluded.checksum,
                updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
        )
        .bind(key.as_str())
        .bind(version.get())
        .bind(&blob_id)
        .bind(content_length as i64)
        .bind(&checksum)
        .bind(updated_at_unix_ms as i64)
        .execute(&self.pool)
        .await?;

        Ok(StoredObject {
            record: ObjectRecord {
                key: key.clone(),
                version,
                blob_id,
                content_length,
                checksum,
                updated_at_unix_ms,
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
            .map(|record| record.version.next())
            .unwrap_or_else(ObjectVersion::initial);

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

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::PersistentObjectStore;
    use crate::domain::types::{ObjectKey, ObjectVersion};
    use crate::storage::repository::{CasWriteOutcome, ObjectRepository};

    #[tokio::test]
    async fn write_survives_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let key = ObjectKey::new("alpha").unwrap();

        let store = PersistentObjectStore::open(temp_dir.path()).await.unwrap();
        let written = store.write(&key, b"hello".to_vec()).await.unwrap();
        assert_eq!(written.record.version, ObjectVersion::initial());
        drop(store);

        let reopened = PersistentObjectStore::open(temp_dir.path()).await.unwrap();
        let loaded = reopened.read(&key).await.unwrap().unwrap();

        assert_eq!(loaded.record.version, ObjectVersion::initial());
        assert_eq!(loaded.value, b"hello".to_vec());
    }

    #[tokio::test]
    async fn cas_reports_mismatch_without_overwriting() {
        let temp_dir = TempDir::new().unwrap();
        let key = ObjectKey::new("beta").unwrap();
        let store = PersistentObjectStore::open(temp_dir.path()).await.unwrap();

        let written = store.write(&key, b"first".to_vec()).await.unwrap();
        let outcome = store
            .cas(
                &key,
                ObjectVersion::try_from(99).unwrap(),
                b"second".to_vec(),
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
        assert_eq!(loaded.value, b"first".to_vec());
    }
}
