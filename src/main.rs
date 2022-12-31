use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

mod api;

#[derive(Debug, Default)]
pub struct MyExecution {}

#[tonic::async_trait]
impl api::Execution for MyExecution {
    type ExecuteStream = ReceiverStream<Result<api::longrunning::Operation, Status>>;

    async fn execute(
        &self,
        _request: Request<api::ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        todo!()
    }

    type WaitExecutionStream = ReceiverStream<Result<api::longrunning::Operation, Status>>;

    async fn wait_execution(
        &self,
        _request: Request<api::WaitExecutionRequest>,
    ) -> Result<Response<Self::WaitExecutionStream>, Status> {
        todo!()
    }
}

#[derive(Debug, Default)]
pub struct MyCAS {}

#[tonic::async_trait]
impl api::ContentAddressableStorage for MyCAS {
    type GetTreeStream = ReceiverStream<Result<api::GetTreeResponse, Status>>;

    async fn get_tree(
        &self,
        _request: Request<api::GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        todo!()
    }

    async fn find_missing_blobs(
        &self,
        _request: Request<api::FindMissingBlobsRequest>,
    ) -> Result<Response<api::FindMissingBlobsResponse>, Status> {
        todo!()
    }

    async fn batch_update_blobs(
        &self,
        _request: Request<api::BatchUpdateBlobsRequest>,
    ) -> Result<Response<api::BatchUpdateBlobsResponse>, Status> {
        todo!()
    }

    async fn batch_read_blobs(
        &self,
        _request: Request<api::BatchReadBlobsRequest>,
    ) -> Result<Response<api::BatchReadBlobsResponse>, Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let exec = MyExecution::default();
    let cas = MyCAS::default();

    Server::builder()
        .add_service(api::ExecutionServer::new(exec))
        .add_service(api::ContentAddressableStorageServer::new(cas))
        .serve(addr)
        .await?;

    Ok(())
}
