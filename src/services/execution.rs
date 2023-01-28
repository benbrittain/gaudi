use crate::{
    api,
    content_storage::{CasError, ContentStorage},
    execution_runner::{ActionError, ExecutionRunner, Stage},
    sandboxed_action::{Mapping, SandboxedAction, StatusCode},
};
use futures::future::BoxFuture;
use prost::Message;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
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

async fn run_action(
    cas: ContentStorage,
    command_digest: api::Digest,
    root_digest: api::Digest,
) -> Result<StatusCode, ActionError> {
    let cmd: api::Command = cas.get_proto("remote-execution", &command_digest).await?;
    let root: api::Directory = cas.get_proto("remote-execution", &root_digest).await?;

    if !cmd.output_paths.is_empty() {
        panic!("output paths is set but we only support v2.0 for now");
    }

    info!("Command: {:#?}", cmd);
    info!("Root: {:#?}", root);
    let env_vars: Vec<(String, String)> = cmd
        .environment_variables
        .iter()
        .map(|ev| (ev.name.clone(), ev.value.clone()))
        .collect();

    let mut mappings = vec![];
    create_mapping(
        &cas,
        root,
        PathBuf::from("/home/ben/workspace/gaudi/sandbox"),
        &mut mappings,
    )
    .await?;

    let mut action = SandboxedAction::new(&cmd.arguments[0])
        .args(&cmd.arguments[..])
        .envs(&env_vars)
        .input_file_mapping(&mappings)
        .input_file("/usr/bin/")
        .input_file("/usr/lib/")
        .input_file("/usr/include/")
        .input_file("/usr/local/include/")
        .input_file("/lib64/ld-linux-x86-64.so.2")
        .input_file("/lib/gcc/x86_64-pc-linux-gnu/")
        .input_file("/usr/include/c++/12.2.0/")
        .input_file("/usr/local/include")
        .input_file("/usr/include/")
        .output_files(
            &cmd.output_files
                .iter()
                .map(PathBuf::from)
                .collect::<Vec<PathBuf>>(),
        );

    info!("Running action...");
    let spawned_action = action.spawn()?;
    spawned_action.status().await.map_err(Into::into)
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

        let action_fut = Box::pin(run_action(self.cas.clone(), command_digest, root_digest));

        let (tx, rx) = mpsc::channel(128);
        let (uuid, mut action_stream) = self.exec_runner.queue(action_fut);

        tokio::spawn(async move {
            while let Some(stage) = action_stream.next().await {
                let op = match stage {
                    Stage::Completed => {
                        let (metadata, result) = create_result();
                        api::Operation {
                            name: uuid.to_string(),
                            done: true,
                            metadata,
                            result,
                        }
                    }
                    _ => todo!(),
                };
                tx.send(Result::<_, Status>::Ok(op)).await.unwrap();
            }
        });

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

fn create_result() -> (Option<prost_types::Any>, Option<api::operation::Result>) {
    let result = api::ActionResult {
        output_files: vec![],
        output_file_symlinks: vec![],
        output_symlinks: vec![],
        output_directories: vec![],
        output_directory_symlinks: vec![],
        exit_code: 0,
        execution_metadata: None,
        stdout_digest: None,
        stderr_digest: None,
        stdout_raw: vec![],
        stderr_raw: vec![],
    };
    let response = api::ExecuteResponse {
        result: Some(result),
        cached_result: false,
        status: Some(api::Status {
            code: 0,
            message: "status ok ben".to_string(),
            details: vec![],
        }),
        server_logs: HashMap::new(),
        message: String::from("TODO BEN"),
    };

    let metadata: api::ExecuteOperationMetadata = api::ExecuteOperationMetadata {
        stage: api::execution_stage::Value::Completed.into(),
        action_digest: None,
        stdout_stream_name: String::from(""),
        stderr_stream_name: String::from(""),
    };

    (
        Some(prost_types::Any {
            type_url: String::from(
                "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteOperationMetadata",
            ),
            value: metadata.encode_to_vec(),
        }),
        Some(api::operation::Result::Response(prost_types::Any {
            type_url: String::from(
                "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteResponse",
            ),
            value: response.encode_to_vec(),
        })),
    )
}
