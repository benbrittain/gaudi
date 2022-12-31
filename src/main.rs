use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, instrument};

mod api;

#[derive(Debug, Default)]
pub struct MyCaps {}

#[tonic::async_trait]
impl api::Capabilities for MyCaps {
    #[instrument]
    async fn get_capabilities(
        &self,
        _request: Request<api::GetCapabilitiesRequest>,
    ) -> Result<Response<api::ServerCapabilities>, Status> {
        todo!()
    }
}

#[derive(Debug, Default)]
pub struct MyExecution {}

#[tonic::async_trait]
impl api::Execution for MyExecution {
    type ExecuteStream = ReceiverStream<Result<api::longrunning::Operation, Status>>;

    #[instrument]
    async fn execute(
        &self,
        _request: Request<api::ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        todo!()
    }

    type WaitExecutionStream = ReceiverStream<Result<api::longrunning::Operation, Status>>;

    #[instrument]
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

    #[instrument]
    async fn get_tree(
        &self,
        _request: Request<api::GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        todo!()
    }

    #[instrument]
    async fn find_missing_blobs(
        &self,
        _request: Request<api::FindMissingBlobsRequest>,
    ) -> Result<Response<api::FindMissingBlobsResponse>, Status> {
        todo!()
    }

    #[instrument]
    async fn batch_update_blobs(
        &self,
        _request: Request<api::BatchUpdateBlobsRequest>,
    ) -> Result<Response<api::BatchUpdateBlobsResponse>, Status> {
        todo!()
    }

    #[instrument]
    async fn batch_read_blobs(
        &self,
        _request: Request<api::BatchReadBlobsRequest>,
    ) -> Result<Response<api::BatchReadBlobsResponse>, Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    info!("Initialized.");

    let addr = "[::1]:8980".parse()?;
    let exec = MyExecution::default();
    let cas = MyCAS::default();
    let caps = MyCaps::default();

    info!("Serving.");
    Server::builder()
        .add_service(api::ExecutionServer::new(exec))
        .add_service(api::ContentAddressableStorageServer::new(cas))
        .add_service(api::CapabilitiesServer::new(caps))
        .serve(addr)
        .await?;

    Ok(())
}
