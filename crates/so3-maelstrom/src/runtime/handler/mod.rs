use std::sync::Arc;

use so3_core::domain::error::{So3Error, So3Result};

use crate::protocol::{CRASH_CODE, ClientRequest, Message, RequestBody, error_response};
use crate::runtime::handler::blob::{handle_blob_fetch, handle_blob_push};
use crate::runtime::handler::client::{handle_client_message, handle_forward};
use crate::runtime::handler::consensus::handle_consensus;
use crate::runtime::handler::metadata::handle_metadata_query;
use crate::runtime::types::{BlobResponse, SharedRuntime};

mod blob;
mod client;
mod consensus;
mod metadata;

pub(super) fn route_or_spawn(shared: &Arc<SharedRuntime>, message: Message<RequestBody>) {
    match &message.body {
        RequestBody::ConsensusOk {
            in_reply_to,
            payload,
        } => {
            if let Some(tx) = shared
                .shared
                .pending_consensus
                .lock()
                .unwrap()
                .remove(in_reply_to)
            {
                let _ = tx.send(Ok(payload.clone()));
            }
        }
        RequestBody::BlobPushOk { in_reply_to } => {
            if let Some(tx) = shared
                .shared
                .pending_blobs
                .lock()
                .unwrap()
                .remove(in_reply_to)
            {
                let _ = tx.send(Ok(BlobResponse::Pushed));
            }
        }
        RequestBody::BlobFetchOk {
            in_reply_to,
            payload,
        } => {
            if let Some(tx) = shared
                .shared
                .pending_blobs
                .lock()
                .unwrap()
                .remove(in_reply_to)
            {
                let _ = tx.send(Ok(BlobResponse::Fetched(payload.clone())));
            }
        }
        RequestBody::MetadataQueryOk {
            in_reply_to,
            payload,
        } => {
            if let Some(tx) = shared
                .shared
                .pending_metadata_queries
                .lock()
                .unwrap()
                .remove(in_reply_to)
            {
                let _ = tx.send(Ok(payload.clone()));
            }
        }
        RequestBody::ForwardOk {
            in_reply_to,
            response,
        } => {
            if let Some(tx) = shared.pending_forwards.lock().unwrap().remove(in_reply_to) {
                let _ = tx.send(Ok(response.clone()));
            }
        }
        RequestBody::Error {
            in_reply_to, text, ..
        } => complete_pending_error(shared, *in_reply_to, text),
        _ => {
            let shared = Arc::clone(shared);
            tokio::spawn(async move {
                if let Err(error) = handle_message(shared, message).await {
                    tracing::error!(%error, "error handling message");
                }
            });
        }
    }
}

fn complete_pending_error(shared: &SharedRuntime, in_reply_to: u64, text: &str) {
    let consensus_tx = shared
        .shared
        .pending_consensus
        .lock()
        .unwrap()
        .remove(&in_reply_to);
    if let Some(tx) = consensus_tx {
        let _ = tx.send(Err(So3Error::InvalidRequest(text.to_owned())));
    } else if let Some(tx) = shared
        .shared
        .pending_metadata_queries
        .lock()
        .unwrap()
        .remove(&in_reply_to)
    {
        let _ = tx.send(Err(So3Error::InvalidRequest(text.to_owned())));
    } else if let Some(tx) = shared.pending_forwards.lock().unwrap().remove(&in_reply_to) {
        let _ = tx.send(Err(So3Error::InvalidRequest(text.to_owned())));
    } else if let Some(tx) = shared
        .shared
        .pending_blobs
        .lock()
        .unwrap()
        .remove(&in_reply_to)
    {
        let _ = tx.send(Err(So3Error::InvalidRequest(text.to_owned())));
    }
}

async fn handle_message(
    shared: Arc<SharedRuntime>,
    message: Message<RequestBody>,
) -> So3Result<()> {
    let src = message.src;
    match message.body {
        RequestBody::Init { msg_id, .. } => shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
            dest: src,
            body: error_response(msg_id, CRASH_CODE, "duplicate init request"),
        }),
        RequestBody::Read {
            msg_id,
            key: Some(key),
        } => handle_client_message(shared, src, msg_id, ClientRequest::Read { key }).await,
        RequestBody::Read { msg_id, key: None } => {
            let response = shared.service.handle_set_read(msg_id).await;
            shared.send_message(&Message {
                src: shared.shared.node_id.clone(),
                dest: src,
                body: response,
            })
        }
        RequestBody::Add { msg_id, element } => {
            let response = shared.service.handle_add(msg_id, element).await;
            shared.send_message(&Message {
                src: shared.shared.node_id.clone(),
                dest: src,
                body: response,
            })
        }
        RequestBody::Write { msg_id, key, value } => {
            handle_client_message(shared, src, msg_id, ClientRequest::Write { key, value }).await
        }
        RequestBody::Cas {
            msg_id,
            key,
            from,
            to,
            create_if_not_exists,
        } => {
            handle_client_message(
                shared,
                src,
                msg_id,
                ClientRequest::Cas {
                    key,
                    from,
                    to,
                    create_if_not_exists,
                },
            )
            .await
        }
        RequestBody::Forward {
            msg_id,
            client_msg_id,
            request,
        } => handle_forward(shared, src, msg_id, client_msg_id, request).await,
        RequestBody::Consensus {
            msg_id,
            rpc,
            payload,
        } => handle_consensus(shared, src, msg_id, rpc, payload).await,
        RequestBody::BlobPush {
            msg_id,
            blob_id,
            payload,
        } => handle_blob_push(shared, src, msg_id, blob_id, payload).await,
        RequestBody::BlobFetch { msg_id, blob_id } => {
            handle_blob_fetch(shared, src, msg_id, blob_id).await
        }
        RequestBody::MetadataQuery { msg_id, payload } => {
            handle_metadata_query(shared, src, msg_id, payload).await
        }
        RequestBody::ForwardOk { .. }
        | RequestBody::ConsensusOk { .. }
        | RequestBody::BlobPushOk { .. }
        | RequestBody::BlobFetchOk { .. }
        | RequestBody::MetadataQueryOk { .. }
        | RequestBody::Error { .. } => Ok(()),
    }
}
