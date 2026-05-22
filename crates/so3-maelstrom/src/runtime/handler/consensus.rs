use std::sync::Arc;

use prost::Message as ProstMessage;

use so3_core::domain::error::{So3Error, So3Result};
use so3_core::proto::consensus::{
    AcceptRequest as ProtoAcceptRequest, ApplyRequest as ProtoApplyRequest,
    CommitRequest as ProtoCommitRequest, PreAcceptRequest as ProtoPreAcceptRequest,
    RecoverRequest as ProtoRecoverRequest,
};
use so3_core::proto::mappers::{
    accept_req_to_domain, accept_res_to_proto, apply_req_to_domain, apply_res_to_proto,
    commit_req_to_domain, commit_res_to_proto, pre_accept_req_to_domain, pre_accept_res_to_proto,
    recover_req_to_domain, recover_res_to_proto,
};
use so3_core::use_case::inbound_consensus::InboundConsensusUseCase;

use crate::protocol::{CRASH_CODE, ConsensusRpc, Message, RequestBody};
use crate::runtime::types::SharedRuntime;

pub(super) async fn handle_consensus(
    shared: Arc<SharedRuntime>,
    sender: String,
    msg_id: u64,
    rpc: ConsensusRpc,
    payload: Vec<u8>,
) -> So3Result<()> {
    let result: So3Result<Vec<u8>> = match rpc {
        ConsensusRpc::PreAccept => {
            match ProtoPreAcceptRequest::decode(payload.as_slice())
                .map_err(|e| So3Error::Serialization(e.to_string()))
                .and_then(pre_accept_req_to_domain)
            {
                Ok(req) => shared
                    .local_handler
                    .pre_accept(req)
                    .await
                    .map(|r| pre_accept_res_to_proto(r).encode_to_vec()),
                Err(e) => Err(e),
            }
        }
        ConsensusRpc::Accept => {
            match ProtoAcceptRequest::decode(payload.as_slice())
                .map_err(|e| So3Error::Serialization(e.to_string()))
                .and_then(accept_req_to_domain)
            {
                Ok(req) => shared
                    .local_handler
                    .accept(req)
                    .await
                    .map(|r| accept_res_to_proto(r).encode_to_vec()),
                Err(e) => Err(e),
            }
        }
        ConsensusRpc::Commit => {
            match ProtoCommitRequest::decode(payload.as_slice())
                .map_err(|e| So3Error::Serialization(e.to_string()))
                .and_then(commit_req_to_domain)
            {
                Ok(req) => shared
                    .local_handler
                    .commit(req)
                    .await
                    .map(|r| commit_res_to_proto(r).encode_to_vec()),
                Err(e) => Err(e),
            }
        }
        ConsensusRpc::Apply => {
            match ProtoApplyRequest::decode(payload.as_slice())
                .map_err(|e| So3Error::Serialization(e.to_string()))
                .and_then(apply_req_to_domain)
            {
                Ok(req) => shared
                    .local_handler
                    .apply(req)
                    .await
                    .map(|r| apply_res_to_proto(r).encode_to_vec()),
                Err(e) => Err(e),
            }
        }
        ConsensusRpc::Recover => {
            match ProtoRecoverRequest::decode(payload.as_slice())
                .map_err(|e| So3Error::Serialization(e.to_string()))
                .and_then(recover_req_to_domain)
            {
                Ok(req) => shared
                    .local_handler
                    .recover(req)
                    .await
                    .map(|r| recover_res_to_proto(r).encode_to_vec()),
                Err(e) => Err(e),
            }
        }
    };

    match result {
        Ok(response_payload) => shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
            dest: sender,
            body: RequestBody::ConsensusOk {
                in_reply_to: msg_id,
                payload: response_payload,
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
