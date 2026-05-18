use std::sync::Arc;

use so3_core::domain::error::So3Result;

use crate::protocol::{ClientRequest, Message, RequestBody, ResponseBody};
use crate::runtime::types::SharedRuntime;

pub(super) async fn handle_client_message(
    shared: Arc<SharedRuntime>,
    client: String,
    msg_id: u64,
    request: ClientRequest,
) -> So3Result<()> {
    log_coordination(&shared, msg_id, client_request_kind(&request), "client");
    let response = execute_local_command(&shared, msg_id, request).await;
    shared.send_message(&Message {
        src: shared.shared.node_id.clone(),
        dest: client,
        body: response,
    })
}

pub(super) async fn handle_forward(
    shared: Arc<SharedRuntime>,
    sender: String,
    msg_id: u64,
    client_msg_id: u64,
    request: ClientRequest,
) -> So3Result<()> {
    log_coordination(
        &shared,
        client_msg_id,
        client_request_kind(&request),
        "forward",
    );
    let response = execute_local_command(&shared, client_msg_id, request).await;
    shared.send_message(&Message {
        src: shared.shared.node_id.clone(),
        dest: sender,
        body: RequestBody::ForwardOk {
            in_reply_to: msg_id,
            response,
        },
    })
}

async fn execute_local_command(
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

fn client_request_kind(request: &ClientRequest) -> &'static str {
    match request {
        ClientRequest::Read { .. } => "read",
        ClientRequest::Write { .. } => "write",
        ClientRequest::Cas { .. } => "cas",
    }
}

fn log_coordination(
    shared: &SharedRuntime,
    msg_id: u64,
    operation: &'static str,
    source: &'static str,
) {
    tracing::info!(
        coordination_event = "client_operation",
        source,
        operation_id = msg_id,
        entry_node = %shared.shared.node_id,
        coordinator_node = %shared.shared.node_id,
        operation,
        consensus_path = "local",
        "maelstrom coordination"
    );
}
