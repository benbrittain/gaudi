use crate::api;
use openat2::*;
use std::io;
use std::os::fd::RawFd;
use std::path::PathBuf;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, instrument};
use uuid::Uuid;

use crate::blob::Blob;

#[derive(Error, Debug)]
pub enum CasError {
    #[error("I/O Error: {0}")]
    IoError(#[from] io::Error),

    #[error("Error with a blob {0}")]
    BlobError(#[from] crate::blob::BlobError),

    #[error("Path passed for CAS root directory is not a directory")]
    NotDirectory,

    #[error("Proto did not decode cleanly")]
    InvalidProto,

    #[error("unknown data store error")]
    Unknown,
}

#[derive(Clone, Copy, Debug)]
pub struct ContentStorage {
    root_fd: RawFd,
}

impl ContentStorage {
    #[instrument]
    pub fn new(root_path: PathBuf) -> Result<Self, CasError> {
        // TODO make a root handle for each instance instead of just one per CAS
        std::fs::create_dir_all(&root_path.join("remote-execution"))?;
        info!("Storage: {}", std::fs::canonicalize(&root_path)?.display());

        let mut how = OpenHow::new(libc::O_CLOEXEC | libc::O_DIRECTORY, 0);
        how.resolve |= ResolveFlags::NO_SYMLINKS;
        let root_fd = openat2(None, root_path, &how)?;

        Ok(ContentStorage { root_fd })
    }

    #[instrument(skip(self))]
    pub async fn get_proto<T: prost::Message + Default>(
        &self,
        instance: &str,
        digest: api::Digest,
    ) -> Result<T, CasError> {
        info!("digest: {:?}", digest);
        let mut blob = self.get_blob(instance, &digest.hash).await?;
        let mut buf = vec![];
        blob.file().read_to_end(&mut buf).await?;
        T::decode(&mut std::io::Cursor::new(buf)).map_err(|_| CasError::InvalidProto)
    }

    #[instrument(skip(self))]
    async fn get_blob(&self, instance: &str, hash: &str) -> Result<Blob, CasError> {
        Blob::open(self.root_fd, instance, hash)
            .await
            .map_err(Into::into)
    }

    /// Write the specified blob of data to the file.
    #[instrument(skip(self, data))]
    pub async fn write_data(
        &self,
        instance: &str,
        uuid: Uuid,
        hash: &str,
        size: usize,
        write_offset: i64,
        finish_write: bool,
        data: &[u8],
    ) -> Result<usize, CasError> {
        let mut blob = self.get_blob(instance, hash).await?;
        // only support really small stuff right now
        assert!(finish_write);

        let bytes_written = blob.file().write(data).await?;
        blob.file().flush().await?;
        Ok(bytes_written)
    }
}
