use crate::{api, content_storage::ContentStorage, execution_runner::ExecutionRunner};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, instrument};

#[derive(Debug, Default)]
pub struct ActionCacheService {}

#[tonic::async_trait]
impl api::ActionCache for ActionCacheService {
    #[instrument(skip_all)]
    async fn get_action_result(
        &self,
        _request: Request<api::GetActionResultRequest>,
    ) -> Result<Response<api::ActionResult>, Status> {
        info!("Not implemented");
        Err(Status::not_found("BWB Not implemented"))
    }
    #[instrument(skip_all)]
    async fn update_action_result(
        &self,
        _request: Request<api::UpdateActionResultRequest>,
    ) -> Result<Response<api::ActionResult>, Status> {
        info!("Not implemented");
        Err(Status::resource_exhausted("BWB No more"))
    }
}
