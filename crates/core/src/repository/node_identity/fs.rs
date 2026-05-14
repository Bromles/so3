use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::fs;
use uuid::Uuid;

use crate::domain::error::{So3Error, So3Result};
use crate::repository::node_identity::interface::NodeIdentityRepository;

const FILE_NAME: &str = "node_id";

pub struct FileSystemNodeIdentityRepository {
    path: PathBuf,
}

impl FileSystemNodeIdentityRepository {
    pub async fn new(dir: impl AsRef<Path>) -> So3Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).await?;
        Ok(Self {
            path: dir.join(FILE_NAME),
        })
    }
}

#[async_trait]
impl NodeIdentityRepository for FileSystemNodeIdentityRepository {
    async fn load(&self) -> So3Result<Option<Uuid>> {
        match fs::read_to_string(&self.path).await {
            Ok(content) => Ok(Some(Uuid::parse_str(content.trim())?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(So3Error::from(e)),
        }
    }

    async fn store(&self, id: Uuid) -> So3Result<()> {
        fs::write(&self.path, id.to_string())
            .await
            .map_err(So3Error::from)
    }
}
