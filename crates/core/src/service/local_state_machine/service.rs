use crate::domain::error::So3Result;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::version::ObjectVersion;
use crate::proto::consensus::{DeleteResult, ReadResult};
use crate::proto::{DeleteResult, ReadResult};
use crate::repository::blob::BlobRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::service::local_state_machine::LocalStateMachine;
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct LocalStateMachineImpl<M: ObjectMetadataRepository, B: BlobRepository> {
    metadata_repository: M,
    blob_repository: B,
}

impl<M: ObjectMetadataRepository, B: BlobRepository> LocalStateMachineImpl<M, B> {
    pub fn new(metadata_repository: M, blob_repository: B) -> Self {
        Self {
            metadata_repository,
            blob_repository,
        }
    }

    async fn execute_read(&self, key: &ObjectKey) -> So3Result<Option<ObjectMetadata>> {
        let Some(metadata) = self.metadata_repository.read(key).await? else {
            return Ok(None);
        };

        Ok(Some(metadata))
    }

    async fn execute_write(
        &self,
        key: &ObjectKey,
        metadata: BlobMetadata,
        last_modified: ObjectLastModified,
    ) -> So3Result<ObjectMetadata> {
        let next_version = self
            .metadata_repository
            .read(key)
            .await?
            .map_or_else(ObjectVersion::initial, |metadata| metadata.version.next());

        let new_metadata = ObjectMetadata {
            key: key.clone(),
            version: next_version,
            blob_metadata: metadata,
            last_modified,
        };

        self.metadata_repository.write(&new_metadata).await?;

        Ok(new_metadata)
    }

    async fn execute_cas(
        &self,
        key: &ObjectKey,
        expected_version: ObjectVersion,
        metadata: BlobMetadata,
        last_modified: ObjectLastModified,
    ) -> So3Result<CasResult> {
        let Some(current_metadata) = self.metadata_repository.read(key).await? else {
            return Ok(CasResult::NotFound);
        };

        if current_metadata.version != expected_version {
            return Ok(CasResult::Mismatch {
                current_version: current_metadata.version,
            });
        }

        let new_metadata = ObjectMetadata {
            key: key.clone(),
            version: current_metadata.version.next(),
            blob_metadata: metadata,
            last_modified,
        };

        self.metadata_repository.write(&new_metadata).await?;

        Ok(CasResult::Applied(new_metadata))
    }

    async fn execute_delete(&self, key: &ObjectKey) -> So3Result<()> {
        let Some(_) = self.metadata_repository.read(key).await? else {
            return Ok(());
        };

        self.metadata_repository.delete(key).await?;

        Ok(())
    }
}

#[async_trait]
impl<M: ObjectMetadataRepository, B: BlobRepository> LocalStateMachine
for LocalStateMachineImpl<M, B>
{
    async fn execute(&self, command: ObjectCommand) -> So3Result<CommandResult> {
        match command {
            ObjectCommand::Read(command) => {
                let metadata = self.execute_read(&command.key).await?;

                Ok(CommandResult::Read(ReadResult { metadata }))
            }
            ObjectCommand::Write(command) => {
                let metadata = self
                    .execute_write(&command.key, command.metadata, command.last_modified)
                    .await?;

                Ok(CommandResult::Write(WriteResult { metadata }))
            }
            ObjectCommand::Cas(command) => match self
                .execute_cas(
                    &command.key,
                    command.expected_version,
                    command.metadata,
                    command.last_modified,
                )
                .await?
            {
                CasResult::Applied(metadata) => {
                    Ok(CommandResult::Cas(CasResult::Applied(metadata)))
                }
                CasResult::NotFound => Ok(CommandResult::Cas(CasResult::NotFound)),
                CasResult::Mismatch { current_version } => {
                    Ok(CommandResult::Cas(CasResult::Mismatch { current_version }))
                }
            },
            ObjectCommand::Delete(command) => {
                self.execute_delete(&command.key).await?;

                Ok(CommandResult::Delete(DeleteResult))
            }
        }
    }
}
