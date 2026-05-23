use async_trait::async_trait;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use sqlx::{Row, SqlitePool, query};
use std::path::Path;
use std::time::Duration;
use tokio::fs;

use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::id::BlobId;
use crate::domain::error::{So3Error, So3Result};
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
use crate::domain::object::version::ObjectVersion;
use crate::repository::metadata::interface::ObjectMetadataRepository;

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_MAX_CONNECTIONS: u32 = 4;
const DATABASE_FILE_NAME: &str = "objects.sqlite";

const CREATE_TABLE_SQL: &str = r"
    CREATE TABLE IF NOT EXISTS objects (
        key                TEXT    PRIMARY KEY,
        version            INTEGER NOT NULL,
        blob_id            TEXT    NOT NULL,
        sha256             BLOB    NOT NULL,
        size               INTEGER NOT NULL,
        last_modified_ms   INTEGER NOT NULL,
        deleted            INTEGER NOT NULL DEFAULT 0
    )
";

const LOAD_SQL: &str = r"
    SELECT key, version, blob_id, sha256, size, last_modified_ms, deleted
    FROM objects WHERE key = ?
";

const UPSERT_SQL: &str = r"
    INSERT INTO objects (key, version, blob_id, sha256, size, last_modified_ms, deleted)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET
        version          = excluded.version,
        blob_id          = excluded.blob_id,
        sha256           = excluded.sha256,
        size             = excluded.size,
        last_modified_ms = excluded.last_modified_ms,
        deleted          = excluded.deleted
";

const MARK_DELETED_SQL: &str = r"
    UPDATE objects SET deleted = 1 WHERE key = ?
";

#[derive(Clone)]
pub struct SqliteObjectMetadataRepository {
    pool: SqlitePool,
}

impl SqliteObjectMetadataRepository {
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

        query(CREATE_TABLE_SQL).execute(&pool).await?;

        Ok(Self { pool })
    }

    fn row_to_metadata(row: &SqliteRow) -> So3Result<ObjectMetadata> {
        let key = ObjectKey::new(row.try_get::<String, _>("key")?)?;
        let version = ObjectVersion::try_from(row.try_get::<i64, _>("version")?)?;
        let blob_id = BlobId::try_from(row.try_get::<&str, _>("blob_id")?)
            .map_err(|e| So3Error::Storage(format!("invalid blob_id in db: {e}")))?;
        let sha256_bytes: Vec<u8> = row.try_get("sha256")?;
        let sha256_arr: [u8; 32] = sha256_bytes
            .try_into()
            .map_err(|_| So3Error::Storage("sha256 in db is not 32 bytes".into()))?;
        let sha256 = Sha256Digest::from_bytes(sha256_arr);
        let size = u64::try_from(row.try_get::<i64, _>("size")?)
            .map_err(|_| So3Error::Storage("negative size in db".into()))?;
        let last_modified_ms = u64::try_from(row.try_get::<i64, _>("last_modified_ms")?)
            .map_err(|_| So3Error::Storage("negative last_modified_ms in db".into()))?;
        let deleted = row.try_get::<bool, _>("deleted")?;

        Ok(ObjectMetadata {
            key,
            version,
            blob_id,
            sha256,
            size,
            last_modified_ms,
            deleted,
        })
    }
}

#[async_trait]
impl ObjectMetadataRepository for SqliteObjectMetadataRepository {
    async fn load(&self, key: &ObjectKey) -> So3Result<Option<ObjectMetadata>> {
        let row = query(LOAD_SQL)
            .bind(key.as_ref())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(Self::row_to_metadata).transpose()
    }

    async fn store(&self, metadata: &ObjectMetadata) -> So3Result<()> {
        query(UPSERT_SQL)
            .bind(metadata.key.as_ref())
            .bind(metadata.version.get())
            .bind(metadata.blob_id.to_string())
            .bind(metadata.sha256.as_bytes().as_slice())
            .bind(
                i64::try_from(metadata.size)
                    .map_err(|_| So3Error::Storage("size overflow".into()))?,
            )
            .bind(
                i64::try_from(metadata.last_modified_ms)
                    .map_err(|_| So3Error::Storage("last_modified_ms overflow".into()))?,
            )
            .bind(metadata.deleted)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete(&self, key: &ObjectKey) -> So3Result<()> {
        query(MARK_DELETED_SQL)
            .bind(key.as_ref())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
