use crate::domain::command::{CommandResult, DeleteCommand, ObjectCommand};
use crate::domain::error::So3Result;
use crate::domain::object_key::ObjectKey;
use crate::repository::blob::BlobRepository;
use crate::use_case::object::use_case::ObjectUseCaseImpl;

impl<B: BlobRepository> ObjectUseCaseImpl<B> {
    pub async fn delete_internal(&self, key: ObjectKey) -> So3Result<()> {
        match self
            .state_machine
            .execute(ObjectCommand::Delete(DeleteCommand { key }))
            .await?
        {
            CommandResult::Delete(_) => Ok(()),
            result => Self::unexpected_result("Delete", &result),
        }
    }
}