use crate::{api, content_storage::ContentStorage, execution_runner::ExecutionRunner};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, instrument};

pub struct OperationsService {}
impl OperationsService {
    pub fn new() -> Self {
        OperationsService {}
    }
}

#[tonic::async_trait]
impl api::Operations for OperationsService {
    async fn list_operations(
        &self,
        request: Request<api::ListOperationsRequest>,
    ) -> Result<Response<api::ListOperationsResponse>, Status> {
        todo!()
    }

    async fn get_operation(
        &self,
        request: Request<api::GetOperationRequest>,
    ) -> Result<Response<api::Operation>, Status> {
        todo!()
    }

    async fn delete_operation(
        &self,
        request: Request<api::DeleteOperationRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn cancel_operation(
        &self,
        request: Request<api::CancelOperationRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn wait_operation(
        &self,
        request: Request<api::WaitOperationRequest>,
    ) -> Result<Response<api::Operation>, Status> {
        todo!()
    }
}
