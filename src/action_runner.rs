use crate::{
    action::platform::linux::syscall_2,
    api,
    content_storage::{CasError, ContentStorage},
};
use futures::future::BoxFuture;
use prost_types::Duration;
use std::{
    ffi::{CStr, CString},
    path::PathBuf,
    ptr,
};
use std::{io, path::Path};
use tracing::{info, instrument, span, Level};

use thiserror::Error;

use crate::action::platform::linux::clone3;

#[derive(Error, Debug)]
pub enum ActionError {
    #[error("I/O: {0}")]
    IoError(#[from] io::Error),
    #[error("CAS: {0}")]
    CasError(#[from] CasError),
    #[error("Unknown")]
    Unknown,
}

#[derive(Debug)]
struct Mapping {
    dest_path: PathBuf,
    source_path: PathBuf,
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

            mapping.push(Mapping { dest_path, source_path });
        }
        for directory_node in &dir.directories {
            info!("dir_node: {:#?}", &directory_node.name);
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

#[instrument]
pub async fn run(
    cas: &ContentStorage,
    command_digest: api::Digest,
    root_digest: api::Digest,
    timeout: Option<Duration>,
) -> Result<(), ActionError> {
    info!("running");
    let cmd: api::Command = cas.get_proto("remote-execution", &command_digest).await?;
    let root: api::Directory = cas.get_proto("remote-execution", &root_digest).await?;
    let mut mappings = vec![];
    create_mapping(
        cas,
        root,
        PathBuf::from("/home/ben/workspace/gaudi/sandbox"),
        &mut mappings,
    )
    .await?;
    info!("Mapping: {:#?}", mappings);
    // Spawn the child that will fork the sandboxed program with fresh namespaces
    spawn_sandbox(mappings, || {})?;
    Ok(())
}

fn mount_sandbox(path: &Path) -> io::Result<()> {
    let target = CString::new(path.to_str().unwrap())?;
    err_check(unsafe {
        libc::mount(
            target.as_ptr(),
            target.as_ptr(),
            ptr::null(),
            libc::MS_BIND | libc::MS_NOSUID,
            ptr::null(),
        )
    })?;
    info!("Mounted sandbox: {}", path.display());

    err_check(unsafe { libc::chdir(target.as_ptr()) })?;

    info!("Entered sandbox.");
    Ok(())
}

fn setup_mount_namespace() -> io::Result<()> {
    let target = CString::new("/").unwrap();
    err_check(unsafe {
        libc::mount(
            ptr::null(),
            target.as_ptr(),
            ptr::null(),
            libc::MS_REC | libc::MS_PRIVATE,
            ptr::null(),
        )
    })?;
    info!("Setup Mount Namespace.");
    Ok(())
}

fn create_empty_file() -> io::Result<()> {
    let path = std::path::Path::new("tmp/empty_file");
    if let Some(prefix) = path.parent() {
        std::fs::create_dir_all(prefix)?;
    }
    std::fs::File::create(path)?;

    Ok(())
}

pub fn path_to_cstring<P: AsRef<Path>>(path: &P) -> Option<std::ffi::CString> {
    path.as_ref()
        .to_str()
        .and_then(|p| std::ffi::CString::new(p).ok())
}

fn mount_mounts(sandbox_path: &Path) -> io::Result<()> {
    let dot = CString::new(".")?;
    err_check(unsafe {
        libc::mount(
            dot.as_ptr(),
            dot.as_ptr(),
            ptr::null(),
            libc::MS_BIND,
            ptr::null(),
        )
    })?;
    info!("Mount point!");

    let path = std::path::Path::new("/bin/pwd");
    let uh_path = std::path::Path::new("bin/pwd");
    let mut full_path = sandbox_path.to_path_buf();
    full_path.push(uh_path);
    if let Some(prefix) = full_path.parent() {
        std::fs::create_dir_all(prefix)?;
    }
    std::fs::File::create(&full_path)?;

    let src = path_to_cstring(&path).unwrap();
    let target = path_to_cstring(&full_path).unwrap();
    info!("mounting: {:?} at {:?}", src, target);

    err_check(unsafe {
        libc::mount(
            src.as_ptr(),
            target.as_ptr(),
            ptr::null(),
            libc::MS_REC | libc::MS_BIND | libc::MS_RDONLY,
            ptr::null(),
        )
    })?;
    info!("check!");

    Ok(())
}

fn err_check(ret: i32) -> io::Result<()> {
    if ret == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn change_root() -> io::Result<()> {
    // make a old root to swap with
    let mut temp = CString::new("old-root-XXXXXX")?;
    unsafe {
        let temp_ptr = temp.into_raw();
        let ret = libc::mkdtemp(temp_ptr);
        if ret == ptr::null_mut() {
            return Err(io::Error::last_os_error());
        }
        temp = CString::from_raw(temp_ptr);
    }
    info!("Made root at {:?}", temp.clone().into_string());

    let dot = CString::new(".")?;
    unsafe {
        err_check(syscall_2(
            libc::SYS_pivot_root as usize,
            dot.as_ptr() as usize,
            temp.as_ptr() as usize,
        ))?;
        err_check(libc::chroot(dot.as_ptr()))?;
        err_check(libc::umount2(temp.as_ptr(), libc::MNT_DETACH))?;
        err_check(libc::rmdir(temp.as_ptr()))?;
    }
    Ok(())
}

fn spawn_sandbox<F>(mappings: Vec<Mapping>, sandboxed_func: F) -> io::Result<()>
where
    F: FnOnce(),
{
    let (child_pid, _pid_fd) = clone3()?;
    let child_span = span!(Level::INFO, "sandbox_process");
    if child_pid == 0 {
        child_span.in_scope(|| {
            //user_namespace()?;
            //network_namespace()?;
            setup_mount_namespace()?;
            mount_sandbox(Path::new("/home/ben/workspace/gaudi/sandbox"))?;
            create_empty_file()?;
            mount_mounts(Path::new("/home/ben/workspace/gaudi/sandbox"))?;
            change_root()?;
            sandboxed_func();
            std::process::exit(0);

            #[allow(unreachable_code)]
            Ok::<(), io::Error>(())
        })?;
    } else {
        info!("Parent");
    }
    Ok(())
}

#[test]
#[tracing_test::traced_test]
fn clone_test() {
    spawn_sandbox().unwrap();
    let path = std::env::current_dir();
    info!("{:?}", path);
    use walkdir::WalkDir;

    for entry in WalkDir::new("/").min_depth(1) {
        println!("{}", entry.unwrap().path().display());
    }
    //let x = std::process::Command::new("pwd")
    //    .env("PATH", "/bin")
    //    .output()
    //    .expect("failed to execute process");
    //eprintln!("{:?}", x);
}
