use serde_json::Value;

use so3_core::consensus::state_machine::ObjectCommandExecutor;
use so3_core::domain::error::So3Error;
use so3_core::domain::{
    CasCommand, CasResult, ObjectCommand, ObjectKey, ObjectResult, ReadCommand, StoredObject,
    WriteCommand,
};
use so3_core::object_server::service::ObjectService;

use crate::protocol::{
    CRASH_CODE, ClientRequest, KEY_DOES_NOT_EXIST_CODE, MALFORMED_REQUEST_CODE,
    PRECONDITION_FAILED_CODE, ResponseBody, error_response,
};

#[derive(Clone)]
pub struct MaelstromService<E: ObjectCommandExecutor> {
    object_service: ObjectService<E>,
}

impl<E: ObjectCommandExecutor> MaelstromService<E> {
    #[must_use]
    pub fn new(object_service: ObjectService<E>) -> Self {
        Self { object_service }
    }
}

impl<E> MaelstromService<E>
where
    E: ObjectCommandExecutor + Clone + Send + Sync + 'static,
{
    #[cfg(test)]
    pub async fn handle(&self, request: crate::protocol::RequestBody) -> ResponseBody {
        use crate::protocol::RequestBody;

        match request {
            RequestBody::Init { msg_id, .. } => error_response(
                msg_id,
                MALFORMED_REQUEST_CODE,
                "init is handled by the maelstrom runtime bootstrap",
            ),
            RequestBody::Read { msg_id, key } => self.handle_read(msg_id, key).await,
            RequestBody::Write { msg_id, key, value } => {
                self.handle_write(msg_id, key, value).await
            }
            RequestBody::Cas {
                msg_id,
                key,
                from,
                to,
                create_if_not_exists,
            } => {
                self.handle_cas(msg_id, key, from, to, create_if_not_exists)
                    .await
            }
            RequestBody::Forward { msg_id, .. }
            | RequestBody::Consensus { msg_id, .. }
            | RequestBody::ForwardOk {
                in_reply_to: msg_id,
                ..
            }
            | RequestBody::ConsensusOk {
                in_reply_to: msg_id,
                ..
            }
            | RequestBody::Error {
                in_reply_to: msg_id,
                ..
            } => error_response(
                msg_id,
                MALFORMED_REQUEST_CODE,
                "internal maelstrom messages are handled by the runtime",
            ),
        }
    }

    #[cfg(test)]
    pub async fn handle_client(&self, msg_id: u64, request: ClientRequest) -> ResponseBody {
        match request {
            ClientRequest::Read { key } => self.handle_read(msg_id, key).await,
            ClientRequest::Write { key, value } => self.handle_write(msg_id, key, value).await,
            ClientRequest::Cas {
                key,
                from,
                to,
                create_if_not_exists,
            } => {
                self.handle_cas(msg_id, key, from, to, create_if_not_exists)
                    .await
            }
        }
    }

    pub async fn prepare_command(
        &self,
        msg_id: u64,
        request: ClientRequest,
    ) -> Result<ObjectCommand, ResponseBody> {
        match request {
            ClientRequest::Read { key } => {
                let key = object_key_from_json(&key).map_err(|error| map_error(msg_id, &error))?;
                Ok(ObjectCommand::Read(ReadCommand { key }))
            }
            ClientRequest::Write { key, value } => {
                let key = object_key_from_json(&key).map_err(|error| map_error(msg_id, &error))?;
                let value = value_to_bytes(&value).map_err(|error| map_error(msg_id, &error))?;
                Ok(ObjectCommand::Write(WriteCommand { key, value }))
            }
            ClientRequest::Cas {
                key,
                from,
                to,
                create_if_not_exists,
            } => {
                let key = object_key_from_json(&key).map_err(|error| map_error(msg_id, &error))?;
                let value = value_to_bytes(&to).map_err(|error| map_error(msg_id, &error))?;
                let current = self
                    .object_service
                    .read(key.clone())
                    .await
                    .map_err(|error| map_error(msg_id, &error))?;

                let Some(current) = current else {
                    if create_if_not_exists {
                        return Ok(ObjectCommand::Write(WriteCommand { key, value }));
                    }

                    return Err(error_response(
                        msg_id,
                        KEY_DOES_NOT_EXIST_CODE,
                        format!("key does not exist: {}", key.as_str()),
                    ));
                };

                let current_value =
                    value_from_object(&current).map_err(|error| map_error(msg_id, &error))?;
                if current_value != from {
                    return Err(error_response(
                        msg_id,
                        PRECONDITION_FAILED_CODE,
                        format!("expected {from} but had {current_value}"),
                    ));
                }

                Ok(ObjectCommand::Cas(CasCommand {
                    key,
                    expected_version: current.record.version,
                    value,
                }))
            }
        }
    }

    pub fn response_from_result(msg_id: u64, result: ObjectResult) -> ResponseBody {
        match result {
            ObjectResult::Read(read) => match read.object {
                Some(object) => match value_from_object(&object) {
                    Ok(value) => ResponseBody::ReadOk {
                        in_reply_to: msg_id,
                        value,
                    },
                    Err(error) => map_error(msg_id, &error),
                },
                None => error_response(msg_id, KEY_DOES_NOT_EXIST_CODE, "key does not exist"),
            },
            ObjectResult::Write(_) => ResponseBody::WriteOk {
                in_reply_to: msg_id,
            },
            ObjectResult::Cas(CasResult::Applied(_)) => ResponseBody::CasOk {
                in_reply_to: msg_id,
            },
            ObjectResult::Cas(CasResult::NotFound) => {
                error_response(msg_id, KEY_DOES_NOT_EXIST_CODE, "key does not exist")
            }
            ObjectResult::Cas(CasResult::Mismatch { current_version }) => error_response(
                msg_id,
                PRECONDITION_FAILED_CODE,
                format!(
                    "version mismatch: current version is {}",
                    current_version.get()
                ),
            ),
        }
    }

    #[cfg(test)]
    async fn handle_read(&self, msg_id: u64, key: Value) -> ResponseBody {
        let key = match object_key_from_json(&key) {
            Ok(key) => key,
            Err(error) => return map_error(msg_id, &error),
        };

        match self.object_service.read(key.clone()).await {
            Ok(Some(object)) => match value_from_object(&object) {
                Ok(value) => ResponseBody::ReadOk {
                    in_reply_to: msg_id,
                    value,
                },
                Err(error) => map_error(msg_id, &error),
            },
            Ok(None) => error_response(
                msg_id,
                KEY_DOES_NOT_EXIST_CODE,
                format!("key does not exist: {}", key.as_str()),
            ),
            Err(error) => map_error(msg_id, &error),
        }
    }

    #[cfg(test)]
    async fn handle_write(&self, msg_id: u64, key: Value, value: Value) -> ResponseBody {
        let key = match object_key_from_json(&key) {
            Ok(key) => key,
            Err(error) => return map_error(msg_id, &error),
        };
        let value = match value_to_bytes(&value) {
            Ok(value) => value,
            Err(error) => return map_error(msg_id, &error),
        };

        match self.object_service.write(key, value).await {
            Ok(_) => ResponseBody::WriteOk {
                in_reply_to: msg_id,
            },
            Err(error) => map_error(msg_id, &error),
        }
    }

    #[cfg(test)]
    async fn handle_cas(
        &self,
        msg_id: u64,
        key: Value,
        from: Value,
        to: Value,
        create_if_not_exists: bool,
    ) -> ResponseBody {
        let key = match object_key_from_json(&key) {
            Ok(key) => key,
            Err(error) => return map_error(msg_id, &error),
        };
        let target_bytes = match value_to_bytes(&to) {
            Ok(bytes) => bytes,
            Err(error) => return map_error(msg_id, &error),
        };

        loop {
            let current = match self.object_service.read(key.clone()).await {
                Ok(current) => current,
                Err(error) => return map_error(msg_id, &error),
            };

            let Some(current) = current else {
                if create_if_not_exists {
                    return match self
                        .object_service
                        .write(key.clone(), target_bytes.clone())
                        .await
                    {
                        Ok(_) => ResponseBody::CasOk {
                            in_reply_to: msg_id,
                        },
                        Err(error) => map_error(msg_id, &error),
                    };
                }

                return error_response(
                    msg_id,
                    KEY_DOES_NOT_EXIST_CODE,
                    format!("key does not exist: {}", key.as_str()),
                );
            };

            let current_value = match value_from_object(&current) {
                Ok(value) => value,
                Err(error) => return map_error(msg_id, &error),
            };
            if current_value != from {
                return error_response(
                    msg_id,
                    PRECONDITION_FAILED_CODE,
                    format!("expected {from} but had {current_value}"),
                );
            }

            match self
                .object_service
                .cas(key.clone(), current.record.version, target_bytes.clone())
                .await
            {
                Ok(CasResult::Applied(_)) => {
                    return ResponseBody::CasOk {
                        in_reply_to: msg_id,
                    };
                }
                Ok(CasResult::Mismatch { .. } | CasResult::NotFound) => {}
                Err(error) => return map_error(msg_id, &error),
            }
        }
    }
}

fn object_key_from_json(key: &Value) -> Result<ObjectKey, So3Error> {
    ObjectKey::new(
        serde_json::to_string(key).map_err(|error| So3Error::Serialization(error.to_string()))?,
    )
}

fn value_to_bytes(value: &Value) -> Result<Vec<u8>, So3Error> {
    serde_json::to_vec(value).map_err(|error| So3Error::Serialization(error.to_string()))
}

fn value_from_object(object: &StoredObject) -> Result<Value, So3Error> {
    serde_json::from_slice(&object.value)
        .map_err(|error| So3Error::Serialization(error.to_string()))
}

fn map_error(in_reply_to: u64, error: &So3Error) -> ResponseBody {
    match error {
        So3Error::NotFound(_) => {
            error_response(in_reply_to, KEY_DOES_NOT_EXIST_CODE, error.to_string())
        }
        So3Error::CasMismatch { .. } => {
            error_response(in_reply_to, PRECONDITION_FAILED_CODE, error.to_string())
        }
        So3Error::InvalidKey
        | So3Error::InvalidVersion(_)
        | So3Error::InvalidRequest(_)
        | So3Error::Serialization(_) => {
            error_response(in_reply_to, MALFORMED_REQUEST_CODE, error.to_string())
        }
        So3Error::Storage(_) | So3Error::Io(_) | So3Error::RpcNotImplemented => {
            error_response(in_reply_to, CRASH_CODE, error.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::TempDir;

    use so3_core::consensus::state_machine::LocalStateMachine;
    use so3_core::object_server::service::ObjectService;
    use so3_core::storage::registry::SqliteFsPersistentObjectRepository;

    use super::MaelstromService;
    use crate::protocol::{
        KEY_DOES_NOT_EXIST_CODE, PRECONDITION_FAILED_CODE, RequestBody, ResponseBody,
    };

    const TEST_MESSAGE_ID: u64 = 1;

    async fn test_service() -> (
        MaelstromService<LocalStateMachine<SqliteFsPersistentObjectRepository>>,
        TempDir,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteFsPersistentObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();

        (
            MaelstromService::new(ObjectService::new(LocalStateMachine::new(repository))),
            temp_dir,
        )
    }

    #[tokio::test]
    async fn write_then_read_roundtrips_json_value() {
        let (service, _temp_dir) = test_service().await;
        let key = json!(42);
        let value = json!({"answer": 42});

        let write = service
            .handle(RequestBody::Write {
                msg_id: TEST_MESSAGE_ID,
                key: key.clone(),
                value: value.clone(),
            })
            .await;
        let read = service
            .handle(RequestBody::Read {
                msg_id: TEST_MESSAGE_ID + 1,
                key,
            })
            .await;

        assert_eq!(
            write,
            ResponseBody::WriteOk {
                in_reply_to: TEST_MESSAGE_ID,
            }
        );
        assert_eq!(
            read,
            ResponseBody::ReadOk {
                in_reply_to: TEST_MESSAGE_ID + 1,
                value,
            }
        );
    }

    #[tokio::test]
    async fn read_missing_key_returns_key_does_not_exist() {
        let (service, _temp_dir) = test_service().await;

        let response = service
            .handle(RequestBody::Read {
                msg_id: TEST_MESSAGE_ID,
                key: json!("missing"),
            })
            .await;

        let ResponseBody::Error { code, .. } = response else {
            panic!("expected error response");
        };
        assert_eq!(code, KEY_DOES_NOT_EXIST_CODE);
    }

    #[tokio::test]
    async fn cas_compares_previous_value_and_updates_atomically() {
        let (service, _temp_dir) = test_service().await;
        let key = json!("alpha");
        let first = json!(1);
        let second = json!(2);

        let _ = service
            .handle(RequestBody::Write {
                msg_id: TEST_MESSAGE_ID,
                key: key.clone(),
                value: first.clone(),
            })
            .await;
        let cas = service
            .handle(RequestBody::Cas {
                msg_id: TEST_MESSAGE_ID + 1,
                key: key.clone(),
                from: first,
                to: second.clone(),
                create_if_not_exists: false,
            })
            .await;
        let read = service
            .handle(RequestBody::Read {
                msg_id: TEST_MESSAGE_ID + 2,
                key,
            })
            .await;

        assert_eq!(
            cas,
            ResponseBody::CasOk {
                in_reply_to: TEST_MESSAGE_ID + 1,
            }
        );
        assert_eq!(
            read,
            ResponseBody::ReadOk {
                in_reply_to: TEST_MESSAGE_ID + 2,
                value: second,
            }
        );
    }

    #[tokio::test]
    async fn cas_mismatch_returns_precondition_failed() {
        let (service, _temp_dir) = test_service().await;
        let key = json!("alpha");

        let _ = service
            .handle(RequestBody::Write {
                msg_id: TEST_MESSAGE_ID,
                key: key.clone(),
                value: json!(1),
            })
            .await;
        let response = service
            .handle(RequestBody::Cas {
                msg_id: TEST_MESSAGE_ID + 1,
                key,
                from: json!(9),
                to: json!(2),
                create_if_not_exists: false,
            })
            .await;

        let ResponseBody::Error { code, .. } = response else {
            panic!("expected error response");
        };
        assert_eq!(code, PRECONDITION_FAILED_CODE);
    }

    #[tokio::test]
    async fn cas_can_create_missing_key_when_requested() {
        let (service, _temp_dir) = test_service().await;
        let key = json!("alpha");
        let value = json!(7);

        let cas = service
            .handle(RequestBody::Cas {
                msg_id: TEST_MESSAGE_ID,
                key: key.clone(),
                from: json!(0),
                to: value.clone(),
                create_if_not_exists: true,
            })
            .await;
        let read = service
            .handle(RequestBody::Read {
                msg_id: TEST_MESSAGE_ID + 1,
                key,
            })
            .await;

        assert_eq!(
            cas,
            ResponseBody::CasOk {
                in_reply_to: TEST_MESSAGE_ID,
            }
        );
        assert_eq!(
            read,
            ResponseBody::ReadOk {
                in_reply_to: TEST_MESSAGE_ID + 1,
                value,
            }
        );
    }
}
