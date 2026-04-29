use crate::domain::command::{
    CasCommand, CommandResult, DeleteCommand, DeleteResult, ObjectCommand, ReadCommand,
    ReadResult, WriteCommand, WriteResult,
};
use crate::domain::error::So3Result;
use crate::service::object::ObjectService;

pub struct LocalStateMachine<S: ObjectService> {
    object_service: S,
}

impl<S: ObjectService> LocalStateMachine<S> {
    pub fn new(object_service: S) -> Self {
        Self { object_service }
    }

    pub async fn execute(&self, command: ObjectCommand) -> So3Result<CommandResult> {
        match command {
            ObjectCommand::Read(command) => self.handle_read(command).await,
            ObjectCommand::Write(command) => self.handle_write(command).await,
            ObjectCommand::Cas(command) => self.handle_cas(command).await,
            ObjectCommand::Delete(command) => self.handle_delete(command).await,
        }
    }

    async fn handle_read(&self, command: ReadCommand) -> So3Result<CommandResult> {
        let stored_object = self.object_service.read(&command.key).await?;

        Ok(CommandResult::Read(ReadResult {
            metadata: stored_object.map(|o| o.metadata),
        }))
    }

    async fn handle_write(&self, command: WriteCommand) -> So3Result<CommandResult> {
        let metadata = self
            .object_service
            .write(&command.key, command.last_modified, command.metadata)
            .await?;

        Ok(CommandResult::Write(WriteResult { metadata }))
    }

    async fn handle_cas(&self, command: CasCommand) -> So3Result<CommandResult> {
        let cas_result = self
            .object_service
            .cas(
                &command.key,
                command.expected_version,
                command.metadata,
                command.last_modified,
            )
            .await?;

        Ok(CommandResult::Cas(cas_result))
    }

    async fn handle_delete(&self, command: DeleteCommand) -> So3Result<CommandResult> {
        self.object_service.delete(&command.key).await?;

        Ok(CommandResult::Delete(DeleteResult))
    }
}
