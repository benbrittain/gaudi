use openat2::{openat2, OpenHow, ResolveFlags};
use std::io;
use std::os::fd::{FromRawFd, RawFd};
use std::path::PathBuf;
use thiserror::Error;
use tokio::{fs, task};
use tracing::{info, instrument};

#[derive(Error, Debug)]
pub enum BlobError {
    #[error("I/O Error: {0}")]
    OpenError(#[from] io::Error),
}

#[derive(Debug)]
pub struct Blob {
    file: tokio::fs::File,
}

async fn asyncify<F, T>(f: F) -> io::Result<T>
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    match task::spawn_blocking(f).await {
        Ok(res) => res,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::Other,
            "background task failed",
        )),
    }
}

impl Blob {
    #[instrument]
    /// Open a Blob object underneath the CAS root_fd.
    ///
    /// TODO convert root_fd + instance to an instance_fd
    pub async fn open(root_fd: RawFd, instance: &str, hash: &str) -> Result<Blob, BlobError> {
        let path: PathBuf = [instance, hash].iter().collect();
        let file = asyncify(move || {
            let mut how = OpenHow::new(
                libc::O_CLOEXEC | libc::O_LARGEFILE | libc::O_CREAT,
                libc::S_IWUSR | libc::S_IXUSR | libc::S_IRUSR,
            );
            how.resolve |= ResolveFlags::NO_SYMLINKS;
            how.resolve |= ResolveFlags::IN_ROOT;
            let fd = openat2(Some(root_fd), path, &how)?;
            info!("Opened FD #{}.", fd);
            Ok(unsafe { fs::File::from_raw_fd(fd) })
        })
        .await?;

        Ok(Blob { file })
    }
}
