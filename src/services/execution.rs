use crate::action_runner;
use crate::{api, content_storage::ContentStorage, execution_runner::ExecutionRunner};
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
        let resp = action_runner::run(&self.cas, command_digest, root_digest, action.timeout).await;
        info!("Resp: {:#?}", resp);

        // TODO this needs to return the actual build status
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
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
