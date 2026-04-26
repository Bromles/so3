use std::fmt::Write as FmtWrite;
use std::fs::File as StdFile;
use std::io;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::fs::File as TokioFile;
use tokio::fs::ReadDir;
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
        let repository = Self { blob_dir };
        fs::create_dir_all(repository.temp_dir()).await?;
        fs::create_dir_all(repository.committed_dir()).await?;
        repository.remove_stale_temp_blobs().await?;

        Ok(repository)
    }

    fn temp_dir(&self) -> PathBuf {
        self.blob_dir.join(TEMP_BLOBS_DIR_NAME)
    }

    fn committed_dir(&self) -> PathBuf {
        self.blob_dir.join(COMMITTED_BLOBS_DIR_NAME)
    }

    fn temp_path(&self, blob_id: &str) -> PathBuf {
        self.temp_dir()
            .join(format!("{blob_id}.{TEMP_FILE_EXTENSION}"))
    }

    fn committed_path(&self, blob_id: &str) -> PathBuf {
        self.committed_dir().join(blob_id)
    }

    async fn remove_stale_temp_blobs(&self) -> So3Result<()> {
        let mut entries = fs::read_dir(self.temp_dir()).await?;
        while let Some(entry) = next_dir_entry(&mut entries).await? {
            fs::remove_file(entry.path()).await?;
        }

        Ok(())
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
        sync_directory(self.committed_dir()).await?;

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

    async fn delete(&self, blob_id: &str) -> So3Result<()> {
        match fs::remove_file(self.committed_path(blob_id)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(So3Error::from(e)),
        }
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

async fn next_dir_entry(entries: &mut ReadDir) -> So3Result<Option<fs::DirEntry>> {
    entries.next_entry().await.map_err(So3Error::from)
}

async fn sync_directory(path: PathBuf) -> So3Result<()> {
    tokio::task::spawn_blocking(move || {
        let directory = open_directory_for_sync(&path)?;
        directory.sync_all()
    })
    .await
    .map_err(|error| So3Error::Io(format!("directory sync task failed: {error}")))?
    .map_err(So3Error::from)
}

#[cfg(windows)]
fn open_directory_for_sync(path: &Path) -> io::Result<StdFile> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
}

#[cfg(not(windows))]
fn open_directory_for_sync(path: &Path) -> io::Result<StdFile> {
    StdFile::open(path)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tokio::fs;

    use super::{COMMITTED_BLOBS_DIR_NAME, FileSystemBlobRepository, TEMP_BLOBS_DIR_NAME};
    use crate::storage::blob::repository::BlobRepository;

    const TEST_PAYLOAD: &[u8] = b"blob-data";
    const STALE_TEMP_BLOB_NAME: &str = "stale.blob.tmp";

    #[tokio::test]
    async fn store_then_load_roundtrips_blob() {
        let temp_dir = TempDir::new().unwrap();
        let repository = FileSystemBlobRepository::new(temp_dir.path())
            .await
            .unwrap();

        let metadata = repository.store(TEST_PAYLOAD).await.unwrap();
        let loaded = repository.load(&metadata.blob_id).await.unwrap();

        assert_eq!(metadata.content_length, TEST_PAYLOAD.len() as u64);
        assert_eq!(loaded, TEST_PAYLOAD.to_vec());
        assert!(
            temp_dir
                .path()
                .join(COMMITTED_BLOBS_DIR_NAME)
                .join(&metadata.blob_id)
                .exists()
        );
    }

    #[tokio::test]
    async fn new_removes_stale_temp_blobs_left_from_interrupted_writes() {
        let temp_dir = TempDir::new().unwrap();
        let temp_blob_dir = temp_dir.path().join(TEMP_BLOBS_DIR_NAME);
        fs::create_dir_all(&temp_blob_dir).await.unwrap();
        fs::write(temp_blob_dir.join(STALE_TEMP_BLOB_NAME), TEST_PAYLOAD)
            .await
            .unwrap();

        let _repository = FileSystemBlobRepository::new(temp_dir.path())
            .await
            .unwrap();

        let mut entries = fs::read_dir(temp_blob_dir).await.unwrap();
        assert!(entries.next_entry().await.unwrap().is_none());
    }
}
