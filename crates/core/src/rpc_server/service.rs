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
        let command_id = request
            .get_ref()
            .command_id
            .as_ref()
            .map_or("<missing>", |command_id| command_id.origin_node_id.as_str());
        let event_size = request
            .get_ref()
            .event
            .as_ref()
            .map_or(0, |event| event.command.len());

        debug!(
            %command_id,
            event_size,
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
        let command_id = request
            .get_ref()
            .command_id
            .as_ref()
            .map_or("<missing>", |command_id| command_id.origin_node_id.as_str());
        let dependency_count = request
            .get_ref()
            .dependencies
            .as_ref()
            .map_or(0, |dependencies| dependencies.commands.len());

        debug!(
            %command_id,
            dependency_count,
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
        let command_id = request
            .get_ref()
            .command_id
            .as_ref()
            .map_or("<missing>", |command_id| command_id.origin_node_id.as_str());
        let dependency_count = request
            .get_ref()
            .dependencies
            .as_ref()
            .map_or(0, |dependencies| dependencies.commands.len());

        debug!(
            %command_id,
            dependency_count,
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
        let command_id = request
            .get_ref()
            .command_id
            .as_ref()
            .map_or("<missing>", |command_id| command_id.origin_node_id.as_str());
        let event_size = request
            .get_ref()
            .event
            .as_ref()
            .map_or(0, |event| event.command.len());

        debug!(
            %command_id,
            event_size,
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
        let command_id = request
            .get_ref()
            .command_id
            .as_ref()
            .map_or("<missing>", |command_id| command_id.origin_node_id.as_str());
        let ballot_round = request
            .get_ref()
            .ballot
            .as_ref()
            .map_or(0, |ballot| ballot.round);

        debug!(
            %command_id,
            ballot_round,
            "received recover request"
        );
        Err(Status::unimplemented(
            "accord recover handling is not implemented yet",
        ))
    }
}
