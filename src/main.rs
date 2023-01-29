use clap::Parser;
use std::{net::SocketAddr, path::PathBuf};
use tonic::transport::Server;
use tracing::{info, instrument};

mod services;
use crate::services::*;

mod action;
mod api;
mod blob;
mod content_storage;
mod execution_runner;
mod sandboxed_action;
use content_storage::ContentStorage;
use execution_runner::ExecutionRunner;

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
    let sandbox_dir = PathBuf::from("/home/ben/workspace/gaudi/sandbox");

    // We rely heavily on openat2
    assert!(openat2::has_openat2());

    info!("Initialized.");

    // generic remote build structures
    let content_storage = ContentStorage::new(cas_dir)?;
    let execution_runner = ExecutionRunner::new();
    //    execution_runner.spawn();

    // gRPC RBE services
    let exec = ExecutionService::new(content_storage.clone(), sandbox_dir, execution_runner);
    let cas = ContentStorageService::default();
    let caps = CapabilitiesService::default();
    let ops = OperationsService::new();
    let action_cache = ActionCacheService::default();
    let byte_stream = BytestreamService::new(content_storage.clone());

    info!("Serving on {}", addr);
    Server::builder()
        .add_service(api::ExecutionServer::new(exec))
        .add_service(api::ContentAddressableStorageServer::new(cas))
        .add_service(api::ActionCacheServer::new(action_cache))
        .add_service(api::ByteStreamServer::new(byte_stream))
        .add_service(api::CapabilitiesServer::new(caps))
        .add_service(api::OperationsServer::new(ops))
        .serve(addr)
        .await?;

    Ok(())
}
