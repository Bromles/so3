use std::sync::Arc;

use so3_core::domain::blob::id::BlobId;
use so3_core::domain::blob::payload::BlobPayload;
use so3_core::domain::error::{So3Error, So3Result};
use so3_core::repository::blob::BlobRepository;

use crate::protocol::{CRASH_CODE, Message, RequestBody};
use crate::runtime::types::SharedRuntime;

pub(super) async fn handle_blob_push(
    shared: Arc<SharedRuntime>,
    sender: String,
    msg_id: u64,
    blob_id: String,
    payload: Vec<u8>,
) -> So3Result<()> {
    let result = match BlobId::try_from(blob_id.as_str()) {
        Ok(blob_id) => {
            let payload = BlobPayload::from_vec(payload);
            shared.local_blobs.store(&blob_id, &payload).await
        }
        Err(error) => Err(So3Error::InvalidRequest(error.to_string())),
    };

    match result {
        Ok(()) => shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
            dest: sender,
            body: RequestBody::BlobPushOk {
                in_reply_to: msg_id,
            },
        }),
        Err(error) => shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
            dest: sender,
            body: RequestBody::Error {
                in_reply_to: msg_id,
                code: CRASH_CODE,
                text: error.to_string(),
            },
        }),
    }
}

pub(super) async fn handle_blob_fetch(
    shared: Arc<SharedRuntime>,
    sender: String,
    msg_id: u64,
    blob_id: String,
) -> So3Result<()> {
    let result = match BlobId::try_from(blob_id.as_str()) {
        Ok(blob_id) => shared
            .local_blobs
            .load(&blob_id)
            .await
            .map(|payload| payload.as_bytes().to_vec()),
        Err(error) => Err(So3Error::InvalidRequest(error.to_string())),
    };

    match result {
        Ok(payload) => shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
            dest: sender,
            body: RequestBody::BlobFetchOk {
                in_reply_to: msg_id,
                payload,
            },
        }),
        Err(error) => shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
            dest: sender,
            body: RequestBody::Error {
                in_reply_to: msg_id,
                code: CRASH_CODE,
                text: error.to_string(),
            },
        }),
    }
}
