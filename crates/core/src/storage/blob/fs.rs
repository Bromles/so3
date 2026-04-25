use std::fmt::Write as FmtWrite;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::fs::File as TokioFile;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::domain::error::{So3Error, So3Result};
use crate::storage::blob::repository::{BlobMetadata, BlobRepository};

// On-disk blob layout.
const TEMP_BLOBS_DIR_NAME: &str = "tmp";
const COMMITTED_BLOBS_DIR_NAME: &str = "committed";
const BLOB_FILE_EXTENSION: &str = "blob";
const TEMP_FILE_EXTENSION: &str = "tmp";

#[derive(Clone)]
pub struct FileSystemBlobRepository {
    blob_dir: PathBuf,
}

impl FileSystemBlobRepository {
    /// # Errors
    ///
    /// Returns an error if the local blob directories cannot be created.
    pub async fn new(blob_dir: impl AsRef<Path>) -> So3Result<Self> {
        let blob_dir = blob_dir.as_ref().to_path_buf();
        fs::create_dir_all(blob_dir.join(TEMP_BLOBS_DIR_NAME)).await?;
        fs::create_dir_all(blob_dir.join(COMMITTED_BLOBS_DIR_NAME)).await?;

        Ok(Self { blob_dir })
    }

    fn temp_path(&self, blob_id: &str) -> PathBuf {
        self.blob_dir
            .join(TEMP_BLOBS_DIR_NAME)
            .join(format!("{blob_id}.{TEMP_FILE_EXTENSION}"))
    }

    fn committed_path(&self, blob_id: &str) -> PathBuf {
        self.blob_dir.join(COMMITTED_BLOBS_DIR_NAME).join(blob_id)
    }
}

#[async_trait]
impl BlobRepository for FileSystemBlobRepository {
    async fn store(&self, value: &[u8]) -> So3Result<BlobMetadata> {
        let blob_id = format!("{}.{}", Uuid::new_v4(), BLOB_FILE_EXTENSION);
        let temp_path = self.temp_path(&blob_id);
        let final_path = self.committed_path(&blob_id);

        let mut file = TokioFile::create(&temp_path).await?;
        file.write_all(value).await?;
        file.sync_all().await?;
        drop(file);

        fs::rename(&temp_path, &final_path).await?;

        Ok(BlobMetadata {
            blob_id,
            content_length: value.len() as u64,
            checksum: checksum_hex(value),
        })
    }

    async fn load(&self, blob_id: &str) -> So3Result<Vec<u8>> {
        fs::read(self.committed_path(blob_id))
            .await
            .map_err(So3Error::from)
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

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::FileSystemBlobRepository;
    use crate::storage::blob::repository::BlobRepository;

    const TEST_PAYLOAD: &[u8] = b"blob-data";

    #[tokio::test]
    async fn store_then_load_roundtrips_blob() {
        let temp_dir = TempDir::new().unwrap();
        let repository = FileSystemBlobRepository::new(temp_dir.path()).await.unwrap();

        let metadata = repository.store(TEST_PAYLOAD).await.unwrap();
        let loaded = repository.load(&metadata.blob_id).await.unwrap();

        assert_eq!(metadata.content_length, TEST_PAYLOAD.len() as u64);
        assert_eq!(loaded, TEST_PAYLOAD.to_vec());
    }
}
