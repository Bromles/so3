use std::sync::Arc;

use async_trait::async_trait;
use prost::Message as ProstMessage;

use so3_core::client::interface::{BlobPeerClient, ConsensusPeerClient};
use so3_core::domain::blob::id::BlobId;
use so3_core::domain::blob::payload::BlobPayload;
use so3_core::domain::consensus::transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use so3_core::domain::error::{So3Error, So3Result};
use so3_core::proto::consensus::{
    AcceptResponse as ProtoAcceptResponse, ApplyResponse as ProtoApplyResponse,
    CommitResponse as ProtoCommitResponse, PreAcceptResponse as ProtoPreAcceptResponse,
    RecoverResponse as ProtoRecoverResponse,
};
use so3_core::proto::mappers::{
    accept_req_to_proto, accept_res_to_domain, apply_req_to_proto, apply_res_to_domain,
    commit_req_to_proto, commit_res_to_domain, pre_accept_req_to_proto, pre_accept_res_to_domain,
    recover_req_to_proto, recover_res_to_domain,
};

use crate::protocol::{ConsensusRpc, Message, RequestBody};
use crate::runtime::types::{BlobResponse, SharedState};

pub(super) struct MaelstromBlobPeerClient {
    pub peer_id: String,
    pub shared: Arc<SharedState>,
}

impl MaelstromBlobPeerClient {
    async fn send_blob_request(
        &self,
        body: impl FnOnce(u64) -> RequestBody,
    ) -> So3Result<BlobResponse> {
        let msg_id = self.shared.next_msg_id();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.shared.pending_blobs.lock().unwrap().insert(msg_id, tx);

        let encoded = serde_json::to_vec(&Message {
            src: self.shared.node_id.clone(),
            dest: self.peer_id.clone(),
            body: body(msg_id),
        })
        .map_err(|e| So3Error::Serialization(e.to_string()))?;

        self.shared
            .output
            .send(encoded)
            .map_err(|_| So3Error::PeerUnavailable("output channel closed".into()))?;

        rx.await
            .map_err(|_| So3Error::PeerUnavailable("blob response channel dropped".into()))?
    }
}

#[async_trait]
impl BlobPeerClient for MaelstromBlobPeerClient {
    async fn push(&self, blob_id: BlobId, payload: &BlobPayload) -> So3Result<()> {
        match self
            .send_blob_request(|msg_id| RequestBody::BlobPush {
                msg_id,
                blob_id: blob_id.to_string(),
                payload: payload.as_bytes().to_vec(),
            })
            .await?
        {
            BlobResponse::Pushed => Ok(()),
            BlobResponse::Fetched(_) => Err(So3Error::InvalidRequest(
                "unexpected blob fetch response for push".into(),
            )),
        }
    }

    async fn fetch(&self, blob_id: &BlobId) -> So3Result<BlobPayload> {
        match self
            .send_blob_request(|msg_id| RequestBody::BlobFetch {
                msg_id,
                blob_id: blob_id.to_string(),
            })
            .await?
        {
            BlobResponse::Fetched(payload) => Ok(BlobPayload::from_vec(payload)),
            BlobResponse::Pushed => Err(So3Error::InvalidRequest(
                "unexpected blob push response for fetch".into(),
            )),
        }
    }
}

pub(super) struct MaelstromConsensusPeerClient {
    pub peer_id: String,
    pub shared: Arc<SharedState>,
}

impl MaelstromConsensusPeerClient {
    async fn send_rpc(&self, rpc: ConsensusRpc, payload: Vec<u8>) -> So3Result<Vec<u8>> {
        let msg_id = self.shared.next_msg_id();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.shared
            .pending_consensus
            .lock()
            .unwrap()
            .insert(msg_id, tx);

        let encoded = serde_json::to_vec(&Message {
            src: self.shared.node_id.clone(),
            dest: self.peer_id.clone(),
            body: RequestBody::Consensus {
                msg_id,
                rpc,
                payload,
            },
        })
        .map_err(|e| So3Error::Serialization(e.to_string()))?;

        self.shared
            .output
            .send(encoded)
            .map_err(|_| So3Error::PeerUnavailable("output channel closed".into()))?;

        rx.await
            .map_err(|_| So3Error::PeerUnavailable("consensus response channel dropped".into()))?
    }
}

#[async_trait]
impl ConsensusPeerClient for MaelstromConsensusPeerClient {
    async fn pre_accept(&self, req: PreAcceptRequest) -> So3Result<PreAcceptResponse> {
        let bytes = self
            .send_rpc(
                ConsensusRpc::PreAccept,
                pre_accept_req_to_proto(req).encode_to_vec(),
            )
            .await?;
        let proto = ProtoPreAcceptResponse::decode(bytes.as_slice())
            .map_err(|e| So3Error::Serialization(e.to_string()))?;
        pre_accept_res_to_domain(proto)
    }

    async fn accept(&self, req: AcceptRequest) -> So3Result<AcceptResponse> {
        let bytes = self
            .send_rpc(
                ConsensusRpc::Accept,
                accept_req_to_proto(req).encode_to_vec(),
            )
            .await?;
        let proto = ProtoAcceptResponse::decode(bytes.as_slice())
            .map_err(|e| So3Error::Serialization(e.to_string()))?;
        accept_res_to_domain(proto)
    }

    async fn commit(&self, req: CommitRequest) -> So3Result<CommitResponse> {
        let bytes = self
            .send_rpc(
                ConsensusRpc::Commit,
                commit_req_to_proto(req).encode_to_vec(),
            )
            .await?;
        let proto = ProtoCommitResponse::decode(bytes.as_slice())
            .map_err(|e| So3Error::Serialization(e.to_string()))?;
        commit_res_to_domain(proto)
    }

    async fn apply(&self, req: ApplyRequest) -> So3Result<ApplyResponse> {
        let bytes = self
            .send_rpc(ConsensusRpc::Apply, apply_req_to_proto(req).encode_to_vec())
            .await?;
        let proto = ProtoApplyResponse::decode(bytes.as_slice())
            .map_err(|e| So3Error::Serialization(e.to_string()))?;
        apply_res_to_domain(proto)
    }

    async fn recover(&self, req: RecoverRequest) -> So3Result<RecoverResponse> {
        let bytes = self
            .send_rpc(
                ConsensusRpc::Recover,
                recover_req_to_proto(req).encode_to_vec(),
            )
            .await?;
        let proto = ProtoRecoverResponse::decode(bytes.as_slice())
            .map_err(|e| So3Error::Serialization(e.to_string()))?;
        recover_res_to_domain(proto)
    }
}
