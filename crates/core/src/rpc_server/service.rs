use tonic::{Request, Response, Status, async_trait};
use tracing::debug;

use crate::rpc_server::proto::consensus_transport_server::ConsensusTransport;
use crate::rpc_server::proto::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};

#[derive(Default)]
pub struct ConsensusTransportService;

#[async_trait]
impl ConsensusTransport for ConsensusTransportService {
    async fn pre_accept(
        &self,
        request: Request<PreAcceptRequest>,
    ) -> Result<Response<PreAcceptResponse>, Status> {
        debug!(
            id_len = request.get_ref().id.len(),
            event_len = request.get_ref().event.len(),
            "received pre_accept request"
        );
        Err(Status::unimplemented(
            "accord pre_accept handling is not implemented yet",
        ))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        debug!(
            id_len = request.get_ref().id.len(),
            event_len = request.get_ref().event.len(),
            "received commit request"
        );
        Err(Status::unimplemented(
            "accord commit handling is not implemented yet",
        ))
    }

    async fn accept(
        &self,
        request: Request<AcceptRequest>,
    ) -> Result<Response<AcceptResponse>, Status> {
        debug!(
            id_len = request.get_ref().id.len(),
            event_len = request.get_ref().event.len(),
            "received accept request"
        );
        Err(Status::unimplemented(
            "accord accept handling is not implemented yet",
        ))
    }

    async fn apply(
        &self,
        request: Request<ApplyRequest>,
    ) -> Result<Response<ApplyResponse>, Status> {
        debug!(
            id_len = request.get_ref().id.len(),
            event_len = request.get_ref().event.len(),
            "received apply request"
        );
        Err(Status::unimplemented(
            "accord apply handling is not implemented yet",
        ))
    }

    async fn recover(
        &self,
        request: Request<RecoverRequest>,
    ) -> Result<Response<RecoverResponse>, Status> {
        debug!(
            id_len = request.get_ref().id.len(),
            event_len = request.get_ref().event.len(),
            "received recover request"
        );
        Err(Status::unimplemented(
            "accord recover handling is not implemented yet",
        ))
    }
}
