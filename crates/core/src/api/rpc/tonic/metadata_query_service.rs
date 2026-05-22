use crate::domain::object::key::ObjectKey;
use crate::proto::metadata_query::metadata_query_server::MetadataQuery as MetadataQueryTrait;
use crate::proto::metadata_query::GetMetadataRequest;
use crate::proto::metadata_query::GetMetadataResponse;
use crate::proto::metadata_query_mappers::metadata_option_to_proto_response;
use crate::use_case::metadata_query::MetadataQueryUseCase;
use async_trait::async_trait;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct MetadataQueryService<M: MetadataQueryUseCase> {
    use_case: Arc<M>,
}

impl<M: MetadataQueryUseCase> MetadataQueryService<M> {
    pub fn new(use_case: Arc<M>) -> Self {
        Self { use_case }
    }
}

#[async_trait]
impl<M: MetadataQueryUseCase> MetadataQueryTrait for MetadataQueryService<M> {
    async fn get_metadata(
        &self,
        request: Request<GetMetadataRequest>,
    ) -> Result<Response<GetMetadataResponse>, Status> {
        let req = request.into_inner();
        let key = ObjectKey::new(req.key)
            .map_err(|e| Status::invalid_argument(format!("invalid key: {e}")))?;

        let result = self
            .use_case
            .query(&key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(metadata_option_to_proto_response(
            result.as_ref(),
        )))
    }
}
