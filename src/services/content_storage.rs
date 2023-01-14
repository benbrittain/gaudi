use crate::{api, content_storage::ContentStorage, execution_runner::ExecutionRunner};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, instrument};

#[derive(Debug, Default)]
pub struct ContentStorageService {}

#[tonic::async_trait]
impl api::ContentAddressableStorage for ContentStorageService {
    type GetTreeStream = ReceiverStream<Result<api::GetTreeResponse, Status>>;

    #[instrument(skip_all)]
    async fn get_tree(
        &self,
        _request: Request<api::GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        info!("");
        todo!()
    }

    #[instrument(skip_all)]
    async fn find_missing_blobs(
        &self,
        request: Request<api::FindMissingBlobsRequest>,
    ) -> Result<Response<api::FindMissingBlobsResponse>, Status> {
        let resp = api::FindMissingBlobsResponse {
            missing_blob_digests: request.get_ref().blob_digests.clone(),
        };
        info!("Find all blobs");
        Ok(Response::new(resp))
    }

    #[instrument(skip_all)]
    async fn batch_update_blobs(
        &self,
        _request: Request<api::BatchUpdateBlobsRequest>,
    ) -> Result<Response<api::BatchUpdateBlobsResponse>, Status> {
        info!("");
        todo!()
    }

    #[instrument(skip_all)]
    async fn batch_read_blobs(
        &self,
        _request: Request<api::BatchReadBlobsRequest>,
    ) -> Result<Response<api::BatchReadBlobsResponse>, Status> {
        info!("");
        todo!()
    }
}
