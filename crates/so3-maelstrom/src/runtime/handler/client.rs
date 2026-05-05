use std::sync::Arc;

use tokio::sync::oneshot;

use so3_core::domain::error::{So3Error, So3Result};

use crate::protocol::{ClientRequest, Message, RequestBody, ResponseBody, CRASH_CODE};
use crate::runtime::types::SharedRuntime;

pub(super) async fn handle_client_message(
    shared: Arc<SharedRuntime>,
    client: String,
    msg_id: u64,
    request: ClientRequest,
) -> So3Result<()> {
    if shared.is_coordinator() {
        let response = execute_leader_command(&shared, msg_id, request).await;
        shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
            dest: client,
            body: response,
        })
    } else {
        let forward_msg_id = shared.shared.next_msg_id();
        let (tx, rx) = oneshot::channel();
        shared
            .pending_forwards
            .lock()
            .unwrap()
            .insert(forward_msg_id, tx);
        shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
            dest: shared.coordinator_id().to_owned(),
            body: RequestBody::Forward {
                msg_id: forward_msg_id,
                client_msg_id: msg_id,
                request,
            },
        })?;
        let response = rx
            .await
            .map_err(|_| So3Error::InvalidRequest("forward response channel dropped".into()))??;
        shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
            dest: client,
            body: response,
        })
    }
}

pub(super) async fn handle_forward(
    shared: Arc<SharedRuntime>,
    sender: String,
    msg_id: u64,
    client_msg_id: u64,
    request: ClientRequest,
) -> So3Result<()> {
    if !shared.is_coordinator() {
        return shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
            dest: sender,
            body: RequestBody::Error {
                in_reply_to: msg_id,
                code: CRASH_CODE,
                text: "only the leader can handle forwarded client requests".to_owned(),
            },
        });
    }

    let response = execute_leader_command(&shared, client_msg_id, request).await;
    shared.send_message(&Message {
        src: shared.shared.node_id.clone(),
        dest: sender,
        body: RequestBody::ForwardOk {
            in_reply_to: msg_id,
            response,
        },
    })
}

async fn execute_leader_command(
    shared: &Arc<SharedRuntime>,
    msg_id: u64,
    request: ClientRequest,
) -> ResponseBody {
    match request {
        ClientRequest::Read { key } => shared.service.handle_read(msg_id, key).await,
        ClientRequest::Write { key, value } => {
            shared.service.handle_write(msg_id, key, value).await
        }
        ClientRequest::Cas {
            key,
            from,
            to,
            create_if_not_exists,
        } => {
            shared
                .service
                .handle_cas(msg_id, key, from, to, create_if_not_exists)
                .await
        }
    }
}
