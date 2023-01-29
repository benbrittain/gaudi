use crate::content_storage::CasError;
use crate::sandboxed_action::SandboxedActionResp;
use futures::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio_stream::Stream;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum ActionError {
    #[error("I/O: {0}")]
    SandboxIoError(#[from] std::io::Error),
    #[error("CAS: {0}")]
    CasError(#[from] CasError),
    #[error("Unknown")]
    Unknown,
}

#[derive(Clone, Debug)]
pub enum Stage {
    Queued,
    Executing,
    Completed(SandboxedActionResp),
}

pub struct ActionStatus {
    future: Pin<Box<dyn Future<Output = Result<SandboxedActionResp, ActionError>> + Send>>,
    stage: Stage,
}

impl Stream for ActionStatus {
    type Item = Stage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Stage::Completed(_) = self.stage {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.future).poll(cx) {
            Poll::Ready(s) => match s {
                Ok(s) => {
                    self.stage = Stage::Completed(s);
                    Poll::Ready(Some(self.stage.clone()))
                }
                Err(_) => todo!(),
            },
            Poll::Pending => match self.stage {
                Stage::Queued => {
                    self.stage = Stage::Executing;
                    Poll::Ready(Some(self.stage.clone()))
                }
                Stage::Executing => Poll::Pending,
                Stage::Completed(_) => Poll::Ready(None),
            },
        }
    }
}

pub struct ExecutionRunner {}

impl ExecutionRunner {
    pub fn new() -> Self {
        ExecutionRunner {}
    }

    pub fn queue<F>(&self, future: F) -> (Uuid, ActionStatus)
    where
        F: Future<Output = Result<SandboxedActionResp, ActionError>> + Send + 'static,
    {
        let uuid = Uuid::new_v4();
        (
            uuid,
            ActionStatus {
                future: Box::pin(future),
                stage: Stage::Queued,
            },
        )
    }
}
