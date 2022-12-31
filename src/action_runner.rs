use crate::api;
use prost_types::Duration;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ActionError {
    #[error("Unknown")]
    Unknown,
}

pub async fn run(
    command_hash: api::Digest,
    root_hash: api::Digest,
    timeout: Option<Duration>,
) -> Result<(), ActionError> {
    tokio::spawn(async move {
        // Process each socket concurrently.
        //   process(socket).await
    });
    Ok(())
}
