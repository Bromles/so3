use std::sync::Arc;

use so3_core::domain::blob::id::BlobId;
use so3_core::domain::error::{So3Error, So3Result};
use so3_core::repository::blob::BlobRepository;
use tokio_stream::StreamExt;

use crate::protocol::{Message, RequestBody, CRASH_CODE};
use crate::runtime::types::SharedRuntime;

pub(super) async fn handle_blob_push(
    shared: Arc<SharedRuntime>,
    sender: String,
    msg_id: u64,
    blob_id: String,
    payload: Vec<u8>,
) -> So3Result<()> {
    let result = async {
        let final_blob_id = BlobId::try_from(blob_id.as_str())
            .map_err(|e| So3Error::InvalidRequest(e.to_string()))?;
        let temp_blob_id = BlobId::new();
        shared
            .local_blobs
            .append_chunk(&temp_blob_id, bytes::Bytes::from(payload))
            .await?;
        shared
            .local_blobs
            .commit_as(&temp_blob_id, &final_blob_id)
            .await
    }
    .await;

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
    let result = async {
        let blob_id = BlobId::try_from(blob_id.as_str())
            .map_err(|e| So3Error::InvalidRequest(e.to_string()))?;
        let mut stream = shared.local_blobs.open_reader(&blob_id).await?;
        let mut buf = Vec::new();
        while let Some(chunk) = stream.next().await {
            buf.extend_from_slice(&chunk?);
        }
        So3Result::Ok(buf)
    }
    .await;

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
