use crate::sandboxed_action::{Mapping, SandboxedAction};
use crate::{
    api,
    content_storage::{CasError, ContentStorage},
    execution_runner::ExecutionRunner,
};
use futures::future::BoxFuture;
use prost_types::Duration;
use std::path::PathBuf;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, instrument};

pub struct ExecutionService {
    cas: ContentStorage,
    exec_runner: ExecutionRunner,
}

impl ExecutionService {
    pub fn new(cas: ContentStorage, exec_runner: ExecutionRunner) -> Self {
        ExecutionService { cas, exec_runner }
    }
}

#[instrument(skip_all)]
fn create_mapping<'a>(
    cas: &'a ContentStorage,
    dir: api::Directory,
    root: PathBuf,
    mapping: &'a mut Vec<Mapping>,
) -> BoxFuture<'a, Result<(), CasError>> {
    Box::pin(async move {
        assert_eq!(dir.symlinks.len(), 0);
        for file in dir.files {
            let mut dest_path = root.clone();
            dest_path.push(&file.name);

            let mut source_path = cas.get_root_path().to_path_buf();
            source_path.push("remote-execution");
            source_path.push(file.digest.expect("must have a digest").hash);

            mapping.push(Mapping {
                dest_path,
                source_path,
            });
        }
        for directory_node in &dir.directories {
            let dir: api::Directory = cas
                .get_proto("remote-execution", directory_node.digest.as_ref().unwrap())
                .await?;
            let mut new_root = root.clone();
            new_root.push(&directory_node.name);
            create_mapping(cas, dir, new_root, mapping).await?;
        }
        Ok(())
    })
}

#[derive(Error, Debug)]
pub enum ActionError {
    #[error("I/O: {0}")]
    SandboxIoError(#[from] std::io::Error),
    #[error("CAS: {0}")]
    CasError(#[from] CasError),
    #[error("Unknown")]
    Unknown,
}

async fn run_action(
    cas: &ContentStorage,
    command_digest: api::Digest,
    root_digest: api::Digest,
    timeout: Option<Duration>,
) -> Result<(), ActionError> {
    let cmd: api::Command = cas.get_proto("remote-execution", &command_digest).await?;
    let root: api::Directory = cas.get_proto("remote-execution", &root_digest).await?;
    let env_vars: Vec<(String, String)> = cmd
        .environment_variables
        .iter()
        .map(|ev| (ev.name.clone(), ev.value.clone()))
        .collect();

    let mut mappings = vec![];
    create_mapping(
        cas,
        root,
        PathBuf::from("/home/ben/workspace/gaudi/sandbox"),
        &mut mappings,
    )
    .await?;

    //    let mut action = SandboxedAction::new("sleep")
    //        .args(&[String::from("sleep")])
    //        .args(&[String::from("5")])
    //        .envs(&env_vars)
    let mut action = SandboxedAction::new(&cmd.arguments[0])
        .args(&cmd.arguments[1..])
        .envs(&env_vars)
        .input_file_mapping(&mappings)
        .input_file("/usr/bin/sleep")
        .input_file("/usr/bin/g++")
        .input_file("/usr/bin/gcc")
        .input_file("/usr/lib/libc.so.6")
        .input_file("/usr/lib/libcap.so.2")
        .input_file("/lib64/ld-linux-x86-64.so.2")
        .output_files(
            &cmd.output_files
                .iter()
                .map(PathBuf::from)
                .collect::<Vec<PathBuf>>(),
        );

    info!("Running action...");
    let spawned_action = action.spawn()?;
    spawned_action.status().await?;
    Ok(())
}

#[tonic::async_trait]
impl api::Execution for ExecutionService {
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

        let resp = run_action(&self.cas, command_digest, root_digest, action.timeout).await;
        info!("Resp: {:#?}", resp);

        // TODO this needs to return the actual build status
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            //while let Some(item) = stream.next().await {
            //}
            let op = api::Operation {
                name: "test".to_string(),
                done: false,
                result: None,
                metadata: None,
                // Some(Box::new(api::ExecuteOperationMetadata {

                // }) as Box<dyn any::Any>),
            };
            //            while let Some(item) = stream.next().await {
            match tx.send(Result::<_, Status>::Ok(op)).await {
                Ok(_) => {
                    // item (server response) was queued to be send to client
                }
                Err(_item) => {
                    // output_stream was build from rx and both are dropped
                    //                      break;
                }
            }
            //           }
            println!("\tclient disconnected");
        });

        //        tx.send(Operation {});
        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(output_stream as Self::ExecuteStream))
    }

    type WaitExecutionStream = ReceiverStream<Result<api::longrunning::Operation, Status>>;

    #[instrument(skip_all)]
    async fn wait_execution(
        &self,
        request: Request<api::WaitExecutionRequest>,
    ) -> Result<Response<Self::WaitExecutionStream>, Status> {
        info!("{:?}", request);
        todo!()
    }
}
