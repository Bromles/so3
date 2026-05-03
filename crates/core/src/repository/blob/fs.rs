use std::fs::File as StdFile;
use std::io;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::fs;
use tokio::fs::File as TokioFile;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::domain::blob::id::BlobId;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::error::{So3Error, So3Result};
use crate::repository::blob::interface::BlobRepository;

const TEMP_DIR: &str = "tmp";
const COMMITTED_DIR: &str = "committed";

#[derive(Clone)]
pub struct FileSystemBlobRepository {
    blob_dir: PathBuf,
}

impl FileSystemBlobRepository {
    pub async fn new(blob_dir: impl AsRef<Path>) -> So3Result<Self> {
        let blob_dir = blob_dir.as_ref().to_path_buf();
        fs::create_dir_all(blob_dir.join(TEMP_DIR)).await?;
        fs::create_dir_all(blob_dir.join(COMMITTED_DIR)).await?;
        let repo = Self { blob_dir };
        repo.remove_stale_temp_files().await?;
        Ok(repo)
    }

    fn committed_path(&self, blob_id: &BlobId) -> PathBuf {
        self.blob_dir.join(COMMITTED_DIR).join(blob_id.to_string())
    }

    fn temp_path(&self) -> PathBuf {
        self.blob_dir
            .join(TEMP_DIR)
            .join(Uuid::new_v4().to_string())
    }

    async fn remove_stale_temp_files(&self) -> So3Result<()> {
        let mut entries = fs::read_dir(self.blob_dir.join(TEMP_DIR)).await?;
        while let Some(entry) = entries.next_entry().await? {
            fs::remove_file(entry.path()).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl BlobRepository for FileSystemBlobRepository {
    async fn store(&self, blob_id: &BlobId, payload: &BlobPayload) -> So3Result<()> {
        let final_path = self.committed_path(blob_id);
        if fs::try_exists(&final_path).await? {
            return Ok(());
        }

        let temp_path = self.temp_path();
        let mut file = TokioFile::create(&temp_path).await?;
        file.write_all(payload.as_bytes()).await?;
        file.sync_all().await?;
        drop(file);

        match fs::rename(&temp_path, &final_path).await {
            Ok(()) => sync_dir(self.blob_dir.join(COMMITTED_DIR)).await,
            Err(_) if fs::try_exists(&final_path).await.unwrap_or(false) => {
                let _ = fs::remove_file(&temp_path).await;
                Ok(())
            }
            Err(e) => Err(So3Error::from(e)),
        }
    }

    async fn load(&self, blob_id: &BlobId) -> So3Result<BlobPayload> {
        let bytes = fs::read(self.committed_path(blob_id)).await?;
        Ok(BlobPayload::from_vec(bytes))
    }

    async fn exists(&self, blob_id: &BlobId) -> So3Result<bool> {
        fs::try_exists(self.committed_path(blob_id))
            .await
            .map_err(So3Error::from)
    }

    async fn delete(&self, blob_id: &BlobId) -> So3Result<()> {
        match fs::remove_file(self.committed_path(blob_id)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(So3Error::from(e)),
        }
    }
}

async fn sync_dir(path: PathBuf) -> So3Result<()> {
    tokio::task::spawn_blocking(move || open_dir_for_sync(&path)?.sync_all())
        .await
        .map_err(|e| So3Error::Io(format!("dir sync task failed: {e}")))?
        .map_err(So3Error::from)
}

#[cfg(windows)]
fn open_dir_for_sync(path: &Path) -> io::Result<StdFile> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
}

#[cfg(not(windows))]
fn open_dir_for_sync(path: &Path) -> io::Result<StdFile> {
    StdFile::open(path)
}
