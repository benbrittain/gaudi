use crate::{
    api,
    content_storage::{CasError, ContentStorage},
    execution_runner::{ActionError, ExecutionRunner, Stage},
    sandboxed_action::{Mapping, SandboxedAction, SandboxedActionResp},
};
use futures::future::BoxFuture;
use prost::Message;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::{collections::HashMap, os::fd::FromRawFd};
use tokio::{io::AsyncReadExt, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use tracing::{info, instrument};

pub struct ExecutionService {
    cas: ContentStorage,
    sandbox_root: PathBuf,
    exec_runner: ExecutionRunner,
}

impl ExecutionService {
    pub fn new(cas: ContentStorage, sandbox_root: PathBuf, exec_runner: ExecutionRunner) -> Self {
        ExecutionService {
            cas,
            sandbox_root,
            exec_runner,
        }
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
) -> Result<SandboxedActionResp, ActionError> {
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
        .input_file("/usr/lib64/")
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

        info!("command digest: {:?}", command_digest);

        let action_fut = Box::pin(run_action(self.cas.clone(), command_digest, root_digest));

        let (tx, rx) = mpsc::channel(128);
        let (uuid, mut action_stream) = self.exec_runner.queue(action_fut);

        let cas = self.cas.clone();
        let sandbox_root = self.sandbox_root.clone();
        tokio::spawn(async move {
            while let Some(stage) = action_stream.next().await {
                info!("=============== STAGE : {:?} =============== ", stage);
                let op = match stage {
                    Stage::Completed(resp) => {
                        info!("Completed: {:?}", resp);
                        let (metadata, result) = create_result(
                            cas.clone(),
                            sandbox_root.clone(),
                            action_digest.clone(),
                            resp,
                        )
                        .await?;
                        api::Operation {
                            name: uuid.to_string(),
                            done: true,
                            metadata,
                            result,
                        }
                    }
                    _ => api::Operation {
                        name: uuid.to_string(),
                        done: false,
                        metadata: None,
                        result: None,
                    },
                };
                info!("Operation: {:?}", op);
                tx.send(Result::<_, Status>::Ok(op)).await.unwrap();
            }
            Ok::<(), CasError>(())
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

pub type ActionResult = (Option<prost_types::Any>, Option<api::operation::Result>);

async fn create_result(
    cas: ContentStorage,
    sandbox_path: PathBuf,
    action_digest: api::Digest,
    resp: SandboxedActionResp,
) -> Result<ActionResult, CasError> {
    let mut output_files = vec![];

    for mapping in &resp.output_paths {
        dbg!(&mapping);
        if mapping.source_path.is_file() {
            let digest = cas
                .add_new_blob_from_file("remote-execution", &mapping.source_path)
                .await?;
            let path = mapping.dest_path.strip_prefix(&sandbox_path).unwrap();
            output_files.push(api::OutputFile {
                path: path.to_str().unwrap().to_string(),
                digest: Some(digest),
                is_executable: false,
                contents: vec![],
                node_properties: None,
            })
        } else {
            todo!()
        }
    }
    let stderr_digest = cas
        .add_new_blob_from_file("remote-execution", &resp.stderr)
        .await?;
    let stdout_digest = cas
        .add_new_blob_from_file("remote-execution", &resp.stdout)
        .await?;
    info!("{:#?}", output_files);
    Ok(format_result(
        output_files,
        action_digest,
        stdout_digest,
        stderr_digest,
        resp,
    ))
}

fn format_result(
    output_files: Vec<api::OutputFile>,
    action_digest: api::Digest,
    stdout_digest: api::Digest,
    stderr_digest: api::Digest,
    resp: SandboxedActionResp,
) -> ActionResult {
    // TODO
    let result = api::ActionResult {
        output_files,
        output_file_symlinks: vec![],
        output_symlinks: vec![],
        output_directories: vec![],
        output_directory_symlinks: vec![],
        exit_code: resp.status_code,
        execution_metadata: None,
        stdout_digest: Some(stdout_digest),
        stderr_digest: Some(stderr_digest),
        stdout_raw: vec![],
        stderr_raw: vec![],
    };
    let response = api::ExecuteResponse {
        result: Some(result),
        cached_result: false,
        status: Some(api::Status {
            code: 0,
            message: "".to_string(),
            details: vec![],
        }),
        server_logs: HashMap::new(),
        message: String::from(""),
    };

    let metadata: api::ExecuteOperationMetadata = api::ExecuteOperationMetadata {
        stage: api::execution_stage::Value::Completed.into(),
        action_digest: Some(action_digest),
        stdout_stream_name: String::from(""),
        stderr_stream_name: String::from(""),
    };

    info!("metadata: {:#?}", metadata);
    info!("response: {:#?}", response);

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
