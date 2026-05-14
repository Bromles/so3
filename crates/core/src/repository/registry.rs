use std::path::Path;

use crate::domain::error::So3Result;
use crate::repository::blob::fs::FileSystemBlobRepository;
use crate::repository::consensus_journal::sqlite::SqliteConsensusJournal;
use crate::repository::metadata::sqlite::SqliteObjectMetadataRepository;

pub struct RepositoryRegistry {
    pub metadata_repository: SqliteObjectMetadataRepository,
    pub blob_repository: FileSystemBlobRepository,
    pub consensus_journal: SqliteConsensusJournal,
}

impl RepositoryRegistry {
    /// # Errors
    ///
    /// Returns an error if any durable local repository component cannot be created or opened.
    pub async fn new(
        metadata_dir: impl AsRef<Path>,
        blob_dir: impl AsRef<Path>,
    ) -> So3Result<Self> {
        let metadata_repository =
            SqliteObjectMetadataRepository::new(metadata_dir.as_ref()).await?;
        let blob_repository = FileSystemBlobRepository::new(blob_dir.as_ref()).await?;
        let consensus_journal = SqliteConsensusJournal::new(metadata_dir.as_ref()).await?;

        Ok(Self {
            metadata_repository,
            blob_repository,
            consensus_journal,
        })
    }
}
