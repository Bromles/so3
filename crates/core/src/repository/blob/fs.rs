use std::fmt::Write as FmtWrite;
use std::fs::File as StdFile;
use std::io;
use std::path::{Path, PathBuf};

use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::error::{So3Error, So3Result};
use crate::repository::blob::interface::BlobRepository;
use async_trait::async_trait;
use sha2::Digest;
use tokio::fs;
use tokio::fs::File as TokioFile;
use tokio::fs::ReadDir;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

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

    fn temp_path(&self, name: &str) -> PathBuf {
        self.temp_dir()
            .join(format!("{name}.{TEMP_FILE_EXTENSION}"))
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
    async fn store(&self, value: BlobPayload) -> So3Result<BlobMetadata> {
        let checksum = Sha256Digest::digest_bytes(&value);
        let blob_id = format!("{}.{BLOB_FILE_EXTENSION}", checksum.to_hex());
        let final_path = self.committed_path(&blob_id);

        // Content-addressed: if the file already exists the bytes are identical, so we can
        // return immediately without writing again.
        if fs::try_exists(&final_path).await? {
            return Ok(BlobMetadata {
                blob_id: blob_id.into(),
                content_length: value.len() as u64,
                checksum_sha256: checksum,
            });
        }

        // Write through a UUID-named temp file to avoid collisions when multiple tasks write
        // the same content concurrently.
        let temp_name = Uuid::new_v4().to_string();
        let temp_path = self.temp_path(&temp_name);
        let mut file = TokioFile::create(&temp_path).await?;
        file.write_all(&value).await?;
        file.sync_all().await?;
        drop(file);

        // Atomic rename into the committed directory.  If a concurrent writer already placed
        // the file while we were writing, that is fine — just discard our temp copy.
        match fs::rename(&temp_path, &final_path).await {
            Ok(()) => {
                sync_directory(self.committed_dir()).await?;
            }
            Err(_) if fs::try_exists(&final_path).await.unwrap_or(false) => {
                let _ = fs::remove_file(&temp_path).await;
            }
            Err(e) => return Err(So3Error::from(e)),
        }

        Ok(BlobMetadata {
            blob_id: blob_id.into(),
            content_length: value.len() as u64,
            checksum_sha256: checksum,
        })
    }

    async fn load(&self, blob_id: &str) -> So3Result<BlobPayload> {
        fs::read(self.committed_path(blob_id))
            .await
            .map(BlobPayload::from)
            .map_err(So3Error::from)
    }

    async fn exists(&self, blob_id: &str) -> So3Result<bool> {
        fs::try_exists(self.committed_path(blob_id))
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
    use super::{FileSystemBlobRepository, COMMITTED_BLOBS_DIR_NAME, TEMP_BLOBS_DIR_NAME};
    use crate::domain::blob::payload::BlobPayload;
    use crate::domain::blobs::BlobPayload;
    use crate::repository::blob::interface::BlobRepository;
    use tempfile::TempDir;
    use tokio::fs;

    const TEST_PAYLOAD: BlobPayload = b"blob-data".into();
    const STALE_TEMP_BLOB_NAME: &str = "stale.blob.tmp";

    #[tokio::test]
    async fn store_then_load_roundtrips_blob() {
        let temp_dir = TempDir::new().unwrap();
        let repository = FileSystemBlobRepository::new(temp_dir.path())
            .await
            .unwrap();

        let metadata = repository.store(TEST_PAYLOAD).await.unwrap();
        let loaded = repository.load(metadata.blob_id).await.unwrap();

        assert_eq!(metadata.content_length, TEST_PAYLOAD.len() as u64);
        assert_eq!(loaded, TEST_PAYLOAD.to_vec());
        // blob_id is now sha256(content).blob — just verify the file exists on disk.
        assert!(
            temp_dir
                .path()
                .join(COMMITTED_BLOBS_DIR_NAME)
                .join(metadata.blob_id.as_str())
                .exists()
        );
        assert!(
            std::path::Path::new(metadata.blob_id.as_ref())
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("blob"))
        );
    }

    #[tokio::test]
    async fn store_is_idempotent_for_same_content() {
        let temp_dir = TempDir::new().unwrap();
        let repository = FileSystemBlobRepository::new(temp_dir.path())
            .await
            .unwrap();

        let first = repository.store(TEST_PAYLOAD).await.unwrap();
        let second = repository.store(TEST_PAYLOAD).await.unwrap();

        // Same content must yield identical blob_id and checksum.
        assert_eq!(first.blob_id, second.blob_id);
        assert_eq!(first.checksum_sha256, second.checksum_sha256);

        // Exactly one file must exist in the committed directory.
        let committed_dir = temp_dir.path().join(COMMITTED_BLOBS_DIR_NAME);
        let mut entries = fs::read_dir(&committed_dir).await.unwrap();
        let mut count = 0usize;
        while entries.next_entry().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(
            count, 1,
            "duplicate blobs must not be written for identical content"
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
