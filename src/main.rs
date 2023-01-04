use std::{net::SocketAddr, path::PathBuf};

use clap::Parser;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, instrument};

mod action;
mod action_runner;
mod api;
mod blob;
mod content_storage;
use content_storage::ContentStorage;

#[derive(Debug, Default)]
pub struct MyCaps {}

#[tonic::async_trait]
impl api::Capabilities for MyCaps {
    #[instrument(skip_all)]
    async fn get_capabilities(
        &self,
        request: Request<api::GetCapabilitiesRequest>,
    ) -> Result<Response<api::ServerCapabilities>, Status> {
        info!("Instance: {}", request.get_ref().instance_name);
        let api_version = api::SemVer {
            major: 2,
            minor: 0,
            patch: 0,
            prerelease: String::default(),
        };

        let cache_capabilities = api::CacheCapabilities {
            digest_functions: vec![api::digest_function::Value::Sha256.into()],
            action_cache_update_capabilities: Some(api::ActionCacheUpdateCapabilities {
                update_enabled: true,
            }),
            cache_priority_capabilities: None,
            max_batch_total_size_bytes: 0,
            symlink_absolute_path_strategy: 0,
            supported_compressors: vec![],
            supported_batch_update_compressors: vec![],
        };

        let exec_caps = api::ExecutionCapabilities {
            digest_function: api::digest_function::Value::Sha256.into(),
            exec_enabled: true,
            execution_priority_capabilities: None,
            supported_node_properties: vec![],
        };

        let caps = api::ServerCapabilities {
            cache_capabilities: Some(cache_capabilities),
            execution_capabilities: Some(exec_caps),
            deprecated_api_version: None,
            low_api_version: Some(api_version.clone()),
            high_api_version: Some(api_version.clone()),
        };
        Ok(Response::new(caps))
    }
}

pub struct MyExecution {
    cas: ContentStorage,
}

impl MyExecution {
    pub fn new(cas: ContentStorage) -> Self {
        MyExecution { cas }
    }
}

#[tonic::async_trait]
impl api::Execution for MyExecution {
    type ExecuteStream = ReceiverStream<Result<api::longrunning::Operation, Status>>;

    #[instrument(skip_all, fields(instance = request.get_ref().instance_name))]
    async fn execute(
        &self,
        request: Request<api::ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        let request = request.into_inner();
        let action_digest = request
            .action_digest
            .ok_or(Status::invalid_argument("no action digest"))?;
        let instance = request.instance_name;

        let action: api::Action = self
            .cas
            .get_proto(&instance, &action_digest)
            .await
            .map_err(|_| Status::invalid_argument("bad action proto"))?;

        info!("Action: {:?}", action);

        let command_digest = action.command_digest.ok_or(Status::invalid_argument(
            "Invalid Action: no command digest",
        ))?;
        let root_digest = action
            .input_root_digest
            .ok_or(Status::invalid_argument("Invalid Action: no root digest"))?;

        info!("command: {:?}", command_digest);
        let resp = action_runner::run(&self.cas, command_digest, root_digest, action.timeout).await;
        info!("Resp: {:#?}", resp);

        // TODO this needs to return the actual build status
        let (tx, rx) = mpsc::channel(128);
        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(output_stream as Self::ExecuteStream))
    }

    type WaitExecutionStream = ReceiverStream<Result<api::longrunning::Operation, Status>>;

    #[instrument(skip_all)]
    async fn wait_execution(
        &self,
        _request: Request<api::WaitExecutionRequest>,
    ) -> Result<Response<Self::WaitExecutionStream>, Status> {
        info!("");
        todo!()
    }
}

#[derive(Debug, Default)]
pub struct MyActionCache {}

#[tonic::async_trait]
impl api::ActionCache for MyActionCache {
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

#[derive(Debug)]
pub struct MyBytestream {
    content_store: ContentStorage,
}

impl MyBytestream {
    pub fn new(content_store: ContentStorage) -> Self {
        MyBytestream { content_store }
    }
}

#[tonic::async_trait]
impl api::ByteStream for MyBytestream {
    type ReadStream = ReceiverStream<Result<api::ReadResponse, Status>>;

    #[instrument(skip_all)]
    async fn read(
        &self,
        _request: Request<api::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        info!("");
        todo!()
    }

    #[instrument(skip_all)]
    async fn write(
        &self,
        request: Request<tonic::Streaming<api::WriteRequest>>,
    ) -> Result<Response<api::WriteResponse>, Status> {
        let mut stream = request.into_inner();
        let mut size: usize = 0;
        if let Some(write_req) = stream.message().await? {
            info!("Name: {:?}", &write_req.resource_name);
            let segments: Vec<&str> = write_req.resource_name.split("/").collect();
            let instance = segments[0];
            assert_eq!("uploads", segments[1]);
            let uuid = uuid::Uuid::parse_str(segments[2])
                .map_err(|_| Status::invalid_argument("not a valid uuid"))?;
            assert_eq!("blobs", segments[3]);
            let hash = segments[4];
            let data_size: usize = segments[5]
                .parse()
                .map_err(|_| Status::invalid_argument("bad size value"))?;

            info!("Writing to blob");
            let bytes_written = self
                .content_store
                .write_data(
                    instance,
                    uuid,
                    hash,
                    data_size,
                    write_req.write_offset,
                    write_req.finish_write,
                    &write_req.data,
                )
                .await
                .map_err(|_| Status::internal("content store could not write data"))?;
            info!("Bytes written: {}", bytes_written);
            size += bytes_written;
        }
        Ok(Response::new(api::WriteResponse {
            committed_size: size as i64,
        }))
    }

    #[instrument(skip_all, fields(resource = _request.get_ref().resource_name))]
    async fn query_write_status(
        &self,
        _request: Request<api::QueryWriteStatusRequest>,
    ) -> Result<Response<api::QueryWriteStatusResponse>, Status> {
        info!("Checking...");
        Err(Status::not_found("BWB TODO"))
    }
}

#[derive(Debug, Default)]
pub struct MyCAS {}

#[tonic::async_trait]
impl api::ContentAddressableStorage for MyCAS {
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

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Address to listen on for remote execution requests.
    #[arg(short, long)]
    addr: SocketAddr,

    /// Storage directory.
    #[arg(short, long)]
    dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let addr = args.addr;
    let cas_dir = args.dir;

    // We rely heavily on openat2
    assert!(openat2::has_openat2());

    info!("Initialized.");

    let content_storage = ContentStorage::new(cas_dir)?;

    let exec = MyExecution::new(content_storage.clone());
    let cas = MyCAS::default();
    let caps = MyCaps::default();
    let action_cache = MyActionCache::default();
    let byte_stream = MyBytestream::new(content_storage.clone());

    info!("Serving on {}", addr);
    Server::builder()
        .add_service(api::ExecutionServer::new(exec))
        .add_service(api::ContentAddressableStorageServer::new(cas))
        .add_service(api::ActionCacheServer::new(action_cache))
        .add_service(api::ByteStreamServer::new(byte_stream))
        .add_service(api::CapabilitiesServer::new(caps))
        .serve(addr)
        .await?;

    Ok(())
}
