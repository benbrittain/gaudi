use crate::content_storage::CasError;
use crate::sandboxed_action::StatusCode;
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

#[derive(Debug)]
pub enum Stage {
    Queued,
    Executing,
    Completed,
}

pub struct ActionStatus {
    future: Pin<Box<dyn Future<Output = Result<StatusCode, ActionError>> + Send>>,
    completed: bool,
}

impl Stream for ActionStatus {
    type Item = Stage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.completed {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.future).poll(cx) {
            Poll::Ready(s) => match s {
                Ok(s) => {
                    self.completed = true;
                    Poll::Ready(Some(Stage::Completed))
                }
                Err(_) => todo!(),
            },
            Poll::Pending => Poll::Pending,
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
        F: Future<Output = Result<StatusCode, ActionError>> + Send + 'static,
    {
        let uuid = Uuid::new_v4();
        (
            uuid,
            ActionStatus {
                future: Box::pin(future),
                completed: false,
            },
        )
    }
}
