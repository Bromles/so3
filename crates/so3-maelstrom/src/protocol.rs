use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const KEY_DOES_NOT_EXIST_CODE: i64 = 20;
pub const PRECONDITION_FAILED_CODE: i64 = 22;
pub const MALFORMED_REQUEST_CODE: i64 = 12;
pub const CRASH_CODE: i64 = 13;
pub const NOT_SUPPORTED_CODE: i64 = 10;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message<T> {
    pub src: String,
    pub dest: String,
    pub body: T,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestBody {
    Init {
        msg_id: u64,
        node_id: String,
        node_ids: Vec<String>,
    },
    Read {
        msg_id: u64,
        key: Value,
    },
    Write {
        msg_id: u64,
        key: Value,
        value: Value,
    },
    Cas {
        msg_id: u64,
        key: Value,
        from: Value,
        to: Value,
        #[serde(default)]
        create_if_not_exists: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseBody {
    InitOk {
        in_reply_to: u64,
    },
    ReadOk {
        in_reply_to: u64,
        value: Value,
    },
    WriteOk {
        in_reply_to: u64,
    },
    CasOk {
        in_reply_to: u64,
    },
    Error {
        in_reply_to: u64,
        code: i64,
        text: String,
    },
}

#[must_use]
pub fn reply(request: &Message<RequestBody>, body: ResponseBody) -> Message<ResponseBody> {
    Message {
        src: request.dest.clone(),
        dest: request.src.clone(),
        body,
    }
}

#[must_use]
pub fn error_response(in_reply_to: u64, code: i64, text: impl Into<String>) -> ResponseBody {
    ResponseBody::Error {
        in_reply_to,
        code,
        text: text.into(),
    }
}
