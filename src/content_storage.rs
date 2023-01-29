use crate::api;
use openat2::*;
use prost::DecodeError;
use sha2::{Digest, Sha256};
use std::io;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs::File;
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

    #[error("Proto did not decode cleanly: {0}")]
    InvalidProto(DecodeError),

    #[error("unknown data store error")]
    Unknown,
}

#[derive(Clone, Debug)]
pub struct ContentStorage {
    root_path: PathBuf,
    root_fd: RawFd,
}

impl ContentStorage {
    #[instrument]
    pub fn new(root_path: PathBuf) -> Result<Self, CasError> {
        let root_path = std::fs::canonicalize(root_path)?;
        // TODO make a root handle for each instance instead of just one per CAS
        std::fs::create_dir_all(&root_path.join("remote-execution"))?;
        info!("Storage: {}", std::fs::canonicalize(&root_path)?.display());

        let mut how = OpenHow::new(libc::O_CLOEXEC | libc::O_DIRECTORY, 0);
        how.resolve |= ResolveFlags::NO_SYMLINKS;
        let root_fd = openat2(None, &root_path, &how)?;

        Ok(ContentStorage { root_path, root_fd })
    }

    pub fn get_root_path(&self) -> &Path {
        &self.root_path
    }

    #[instrument(skip(self))]
    pub async fn get_proto<T: prost::Message + Default>(
        &self,
        instance: &str,
        digest: &api::Digest,
    ) -> Result<T, CasError> {
        info!("digest: {:?}", digest);
        let mut blob = self.get_blob(instance, &digest.hash).await?;
        let mut buf = vec![];
        blob.file().read_to_end(&mut buf).await?;
        T::decode(&mut std::io::Cursor::new(buf)).map_err(|e| CasError::InvalidProto(e))
    }

    #[instrument(skip(self))]
    async fn get_blob(&self, instance: &str, hash: &str) -> Result<Blob, CasError> {
        Blob::open(self.root_fd, instance, hash)
            .await
            .map_err(Into::into)
    }

    /// Add a blob from a file location
    #[instrument(skip(self))]
    pub async fn add_new_blob_from_file(
        &self,
        instance: &str,
        path: &Path,
    ) -> Result<api::Digest, CasError> {
        info!("Reading: {}", path.display());
        let mut file = File::open(path).await?;
        let mut buf = vec![];
        file.read_to_end(&mut buf).await?;

        let mut hasher = Sha256::new();
        hasher.update(&buf);

        let mut hash_buf = hasher.finalize();
        let raw = b"\xab\xcd\x12\x34";
        let hex_hash = base16ct::lower::encode_string(&hash_buf);
        info!("hash: {}", hex_hash);
        let mut blob = self.get_blob(instance, &hex_hash).await?;

        let _ = blob.file().write(&buf).await?;
        blob.file().flush().await?;
        Ok(api::Digest {
            size_bytes: buf.len() as i64,
            hash: hex_hash.to_string(),
        })
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

    #[instrument(skip(self))]
    pub async fn read_to_end(&self, instance: &str, hash: &str) -> Result<Vec<u8>, CasError> {
        let mut buf = vec![];
        let mut blob = self.get_blob(instance, hash).await?;
        blob.file().read_to_end(&mut buf).await?;
        Ok(buf)
    }
}
