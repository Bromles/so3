#[cfg(not(windows))]
use std::fs::File as StdFile;
use std::io;
use std::path::{Path, PathBuf};

use crate::domain::blob::id::BlobId;
use crate::domain::blob::stream::BlobStream;
use crate::domain::error::{So3Error, So3Result};
use crate::repository::blob::interface::BlobRepository;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;

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

    fn streaming_temp_path(&self, blob_id: &BlobId) -> PathBuf {
        self.blob_dir.join(TEMP_DIR).join(blob_id.to_string())
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
    async fn append_chunk(&self, blob_id: &BlobId, chunk: Bytes) -> So3Result<()> {
        if fs::try_exists(self.committed_path(blob_id)).await? {
            return Ok(());
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.streaming_temp_path(blob_id))
            .await?;
        file.write_all(&chunk).await?;
        Ok(())
    }

    async fn commit(&self, blob_id: &BlobId) -> So3Result<()> {
        self.commit_as(blob_id, blob_id).await
    }

    async fn commit_as(&self, temp_blob_id: &BlobId, final_blob_id: &BlobId) -> So3Result<()> {
        let tmp_path = self.streaming_temp_path(temp_blob_id);
        let committed_tmp_path = self.committed_path(temp_blob_id);
        let final_path = self.committed_path(final_blob_id);

        if fs::try_exists(&final_path).await.unwrap_or(false) {
            let _ = fs::remove_file(&tmp_path).await;
            let _ = fs::remove_file(&committed_tmp_path).await;
            return Ok(());
        }

        let source_path = if fs::try_exists(&tmp_path).await.unwrap_or(false) {
            let file = fs::OpenOptions::new().write(true).open(&tmp_path).await?;
            file.sync_all().await?;
            drop(file);
            tmp_path
        } else if fs::try_exists(&committed_tmp_path).await.unwrap_or(false) {
            committed_tmp_path
        } else {
            return Err(So3Error::Io(format!("temp blob not found: {temp_blob_id}")));
        };

        match fs::rename(&source_path, &final_path).await {
            Ok(()) => sync_dir(self.blob_dir.join(COMMITTED_DIR)).await,
            Err(_) if fs::try_exists(&final_path).await.unwrap_or(false) => {
                let _ = fs::remove_file(&source_path).await;
                Ok(())
            }
            Err(e) => Err(So3Error::from(e)),
        }
    }

    async fn abort(&self, blob_id: &BlobId) -> So3Result<()> {
        match fs::remove_file(self.streaming_temp_path(blob_id)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(So3Error::from(e)),
        }
    }

    async fn open_reader(&self, blob_id: &BlobId) -> So3Result<BlobStream> {
        let file = fs::File::open(self.committed_path(blob_id)).await?;
        let stream = ReaderStream::new(file).map(|r| r.map_err(So3Error::from));
        Ok(BlobStream::new(stream))
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

#[cfg(windows)]
async fn sync_dir(_path: PathBuf) -> So3Result<()> {
    Ok(())
}

#[cfg(not(windows))]
async fn sync_dir(path: PathBuf) -> So3Result<()> {
    tokio::task::spawn_blocking(move || open_dir_for_sync(&path)?.sync_all())
        .await
        .map_err(|e| So3Error::Io(format!("dir sync task failed: {e}")))?
        .map_err(So3Error::from)
}

#[cfg(not(windows))]
fn open_dir_for_sync(path: &Path) -> io::Result<StdFile> {
    StdFile::open(path)
}
