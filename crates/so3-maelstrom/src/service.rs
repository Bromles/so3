use bytes::Bytes;
use serde_json::Value;

use so3_core::domain::blob::stream::BlobStream;
use so3_core::domain::command::CasResult;
use so3_core::domain::error::{So3Error, So3Result};
use so3_core::domain::object::key::ObjectKey;
use so3_core::domain::object::version::ObjectVersion;
use so3_core::use_case::object::ObjectUseCase;

use crate::protocol::{
    error_response, ResponseBody, CRASH_CODE, KEY_DOES_NOT_EXIST_CODE,
    MALFORMED_REQUEST_CODE, PRECONDITION_FAILED_CODE,
};

const SET_KEY_JSON: &str = "\"__set__\"";

fn set_object_key() -> So3Result<ObjectKey> {
    ObjectKey::new(SET_KEY_JSON.to_string())
        .map_err(|e| So3Error::InvalidRequest(format!("invalid set key: {e}")))
}

pub struct MaelstromService<O: ObjectUseCase> {
    object_use_case: O,
}

impl<O: ObjectUseCase> MaelstromService<O> {
    pub fn new(object_use_case: O) -> Self {
        Self { object_use_case }
    }

    pub async fn handle_read(&self, msg_id: u64, key: Value) -> ResponseBody {
        let key = match object_key_from_json(&key) {
            Ok(k) => k,
            Err(e) => return map_error(msg_id, &e),
        };
        match self.object_use_case.read(&key).await {
            Ok(Some(obj)) => match collect_blob(obj.blob).await {
                Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                    Ok(value) => ResponseBody::ReadOk {
                        in_reply_to: msg_id,
                        value,
                    },
                    Err(e) => map_error(msg_id, &So3Error::Serialization(e.to_string())),
                },
                Err(e) => map_error(msg_id, &e),
            },
            Ok(None) => error_response(msg_id, KEY_DOES_NOT_EXIST_CODE, "key does not exist"),
            Err(e) => map_error(msg_id, &e),
        }
    }

    pub async fn handle_write(&self, msg_id: u64, key: Value, value: Value) -> ResponseBody {
        let key = match object_key_from_json(&key) {
            Ok(k) => k,
            Err(e) => return map_error(msg_id, &e),
        };
        let bytes = match value_to_bytes(&value) {
            Ok(b) => b,
            Err(e) => return map_error(msg_id, &e),
        };
        let stream = bytes_to_stream(bytes);
        match self.object_use_case.write(key, stream).await {
            Ok(_) => ResponseBody::WriteOk {
                in_reply_to: msg_id,
            },
            Err(e) => map_error(msg_id, &e),
        }
    }

    pub async fn handle_cas(
        &self,
        msg_id: u64,
        key: Value,
        from: Value,
        to: Value,
        create_if_not_exists: bool,
    ) -> ResponseBody {
        let key = match object_key_from_json(&key) {
            Ok(k) => k,
            Err(e) => return map_error(msg_id, &e),
        };
        let to_bytes = match value_to_bytes(&to) {
            Ok(b) => b,
            Err(e) => return map_error(msg_id, &e),
        };

        loop {
            let current = match self.object_use_case.read(&key).await {
                Ok(c) => c,
                Err(e) => return map_error(msg_id, &e),
            };

            let Some(current_obj) = current else {
                if create_if_not_exists {
                    return match self
                        .object_use_case
                        .write(key.clone(), bytes_to_stream(to_bytes.clone()))
                        .await
                    {
                        Ok(_) => ResponseBody::CasOk {
                            in_reply_to: msg_id,
                        },
                        Err(e) => map_error(msg_id, &e),
                    };
                }
                return error_response(msg_id, KEY_DOES_NOT_EXIST_CODE, "key does not exist");
            };

            let blob_bytes = match collect_blob(current_obj.blob).await {
                Ok(b) => b,
                Err(e) => return map_error(msg_id, &e),
            };
            let current_value = match serde_json::from_slice::<Value>(&blob_bytes) {
                Ok(v) => v,
                Err(e) => return map_error(msg_id, &So3Error::Serialization(e.to_string())),
            };
            if current_value != from {
                return error_response(
                    msg_id,
                    PRECONDITION_FAILED_CODE,
                    format!("expected {from} but had {current_value}"),
                );
            }

            match self
                .object_use_case
                .cas(
                    key.clone(),
                    current_obj.metadata.version,
                    bytes_to_stream(to_bytes.clone()),
                )
                .await
            {
                Ok(CasResult::Updated(_)) => {
                    return ResponseBody::CasOk {
                        in_reply_to: msg_id,
                    };
                }
                Ok(CasResult::Conflict { .. }) => continue,
                Err(e) => return map_error(msg_id, &e),
            }
        }
    }

    pub async fn handle_set_read(&self, msg_id: u64) -> ResponseBody {
        let key = match set_object_key() {
            Ok(k) => k,
            Err(e) => return map_error(msg_id, &e),
        };
        match self.object_use_case.read(&key).await {
            Ok(Some(obj)) => match collect_blob(obj.blob).await {
                Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                    Ok(value) => ResponseBody::ReadOk {
                        in_reply_to: msg_id,
                        value,
                    },
                    Err(e) => map_error(msg_id, &So3Error::Serialization(e.to_string())),
                },
                Err(e) => map_error(msg_id, &e),
            },
            Ok(None) => ResponseBody::ReadOk {
                in_reply_to: msg_id,
                value: Value::Array(vec![]),
            },
            Err(e) => map_error(msg_id, &e),
        }
    }

    pub async fn handle_add(&self, msg_id: u64, element: Value) -> ResponseBody {
        let key = match set_object_key() {
            Ok(k) => k,
            Err(e) => return map_error(msg_id, &e),
        };

        loop {
            let (current_version, current_set) = match self.read_current_set(&key).await {
                Ok(Some((v, s))) => (v, s),
                Ok(None) => (ObjectVersion::initial(), vec![]),
                Err(e) => return map_error(msg_id, &e),
            };

            if current_set.contains(&element) {
                return ResponseBody::AddOk {
                    in_reply_to: msg_id,
                };
            }

            let mut new_set = current_set;
            new_set.push(element.clone());
            let bytes = match value_to_bytes(&Value::Array(new_set)) {
                Ok(b) => b,
                Err(e) => return map_error(msg_id, &e),
            };

            match self
                .object_use_case
                .cas(key.clone(), current_version, bytes_to_stream(bytes))
                .await
            {
                Ok(CasResult::Updated(_)) => {
                    return ResponseBody::AddOk {
                        in_reply_to: msg_id,
                    };
                }
                Ok(CasResult::Conflict { .. }) => continue,
                Err(e) => return map_error(msg_id, &e),
            }
        }
    }

    async fn read_current_set(
        &self,
        key: &ObjectKey,
    ) -> So3Result<Option<(ObjectVersion, Vec<Value>)>> {
        match self.object_use_case.read(key).await {
            Ok(Some(obj)) => {
                let bytes = collect_blob(obj.blob).await?;
                match serde_json::from_slice::<Value>(&bytes) {
                    Ok(Value::Array(arr)) => Ok(Some((obj.metadata.version, arr))),
                    Ok(_) => Err(So3Error::Storage("set value is not an array".into())),
                    Err(e) => Err(So3Error::Serialization(e.to_string())),
                }
            }
            Ok(None) | Err(So3Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

fn bytes_to_stream(bytes: Vec<u8>) -> BlobStream {
    BlobStream::new(tokio_stream::iter(std::iter::once(Ok(Bytes::from(bytes)))))
}

async fn collect_blob(mut stream: BlobStream) -> So3Result<Vec<u8>> {
    use tokio_stream::StreamExt;
    let mut buf = Vec::new();
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk?);
    }
    Ok(buf)
}

fn object_key_from_json(key: &Value) -> So3Result<ObjectKey> {
    ObjectKey::new(serde_json::to_string(key).map_err(|e| So3Error::Serialization(e.to_string()))?)
}

fn value_to_bytes(value: &Value) -> So3Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| So3Error::Serialization(e.to_string()))
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
        So3Error::Storage(_) | So3Error::Io(_) | So3Error::PeerUnavailable(_) => {
            error_response(in_reply_to, CRASH_CODE, error.to_string())
        }
    }
}
