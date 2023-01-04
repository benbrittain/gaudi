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
use std::{
    io::Write,
    os::fd::{FromRawFd, IntoRawFd, RawFd},
};
use tracing::{info, instrument, span, Level};

use thiserror::Error;

use crate::action::platform::linux::clone3;

#[derive(Error, Debug)]
pub enum ActionError {
    #[error("I/O: {0}")]
    SandboxIoError(#[from] io::Error),
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

fn add_direct_mapping(mappings: &mut Vec<Mapping>, path: &str) {
    mappings.push(Mapping {
        dest_path: PathBuf::from(format!("/home/ben/workspace/gaudi/sandbox/{}", path)),
        source_path: PathBuf::from(path),
    });
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

    // TODO set up build environment better
    add_direct_mapping(&mut mappings, "/usr/bin/gcc");
    add_direct_mapping(&mut mappings, "/usr/lib/libc.so.6");
    add_direct_mapping(&mut mappings, "/usr/lib/libcap.so.2");
    add_direct_mapping(&mut mappings, "/lib64/ld-linux-x86-64.so.2");
    info!("Mapping: {:#?}", mappings);

    // Spawn the child that will fork the sandboxed program with fresh namespaces
    spawn_sandbox(mappings, || {
        info!("In sandbox");
        info!("Command: {:#?}", cmd);

        // Collect Env variables for the new command
        let env_vars: Vec<(String, String)> = cmd
            .environment_variables
            .iter()
            .map(|ev| (ev.name.clone(), ev.value.clone()))
            .collect();

        // Create directories for all output files
        for output in cmd.output_files.iter().map(PathBuf::from) {
            if let Some(prefix) = output.parent() {
                std::fs::create_dir_all(prefix).unwrap();
            }
        }

        unsafe {
            libc::setpgid(0, 0);
            libc::umask(022);
        }

        let cmd = std::process::Command::new(&cmd.arguments[0])
            .args(&cmd.arguments[1..])
            .envs(env_vars)
            .output()
            .expect("failed to execute process");

        info!("Command: {:?}", std::str::from_utf8(&cmd.stdout));
    })?;
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

#[instrument]
fn setup_user_namespace(outer_uid: u32, outer_gid: u32) -> io::Result<()> {
    let inner_uid = outer_uid;
    let inner_gid = outer_gid;

    let path = Path::new("/proc/self/setgroups");

    if path.exists() {
        info!("proc setgroups exists");
        let mut map = std::fs::OpenOptions::new()
            .write(true)
            .open("/proc/self/setgroups")?;
        map.write_all(b"deny\n")?;
    }

    let mut uid_map = std::fs::OpenOptions::new()
        .write(true)
        .open("/proc/self/uid_map")?;
    uid_map.write_all(format!("{} {} 1\n", inner_uid, outer_uid).as_bytes())?;
    let mut gid_map = std::fs::OpenOptions::new()
        .write(true)
        .open("/proc/self/gid_map")?;
    gid_map.write_all(format!("{} {} 1\n", inner_gid, outer_gid).as_bytes())?;

    info!("Setup User Namespace.");
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
    std::fs::create_dir_all("tmp/")?;
    let path = std::path::Path::new("tmp/empty_file");
    std::fs::File::create(path)?;
    Ok(())
}

pub fn path_to_cstring<P: AsRef<Path>>(path: &P) -> Option<std::ffi::CString> {
    path.as_ref()
        .to_str()
        .and_then(|p| std::ffi::CString::new(p).ok())
}

fn mount_mounts(path: &Path, mount_mapping: Vec<Mapping>) -> io::Result<()> {
    // let empty_file = CString::new("/home/ben/workspace/gaudi/sandbox/tmp/empty_file")?;

    // let dot = path_to_cstring(&path).unwrap();
    // err_check(unsafe {
    //     libc::mount(
    //         dot.as_ptr(),
    //         dot.as_ptr(),
    //         ptr::null(),
    //         libc::MS_BIND,
    //         ptr::null(),
    //     )
    // })?;
    info!("Mount point!");

    for mount in mount_mapping {
        info!(
            "Binding {} -> {}",
            mount.source_path.display(),
            mount.dest_path.display()
        );

        // create a file to bind against at the depth
        if let Some(prefix) = mount.dest_path.parent() {
            std::fs::create_dir_all(prefix)?;
        }
        std::fs::File::create(&mount.dest_path)?;

        let src = path_to_cstring(&mount.source_path).unwrap();
        let target = path_to_cstring(&mount.dest_path).unwrap();
        //err_check(unsafe {
        //    libc::link(empty_file.as_ptr(), target.as_ptr())
        //})?;
        //info!("linked file");

        err_check(unsafe {
            libc::mount(
                src.as_ptr(),
                target.as_ptr(),
                ptr::null(),
                //libc::MS_BIND | libc::MS_REC | libc::MS_RDONLY,
                libc::MS_BIND | libc::MS_REC,
                ptr::null(),
            )
        })?;
    }

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

fn mount_dev() -> io::Result<()> {
    std::fs::create_dir_all("dev")?;
    std::fs::File::create("dev/null")?;
    for dev in ["/dev/null"].map(PathBuf::from) {
        let dev_mnt = path_to_cstring(&dev).unwrap();
        err_check(unsafe {
            libc::mount(
                dev_mnt.as_ptr(),
                dev_mnt.as_bytes()[1..].as_ptr() as *const i8,
                ptr::null(),
                libc::MS_BIND,
                ptr::null(),
            )
        })?;
    }
    Ok(())
}
fn mount_proc() -> io::Result<()> {
    let proc_mnt = CString::new("/proc")?;
    let proc = CString::new("proc")?;
    Ok(err_check(unsafe {
        libc::mount(
            proc_mnt.as_ptr(),
            proc_mnt.as_ptr(),
            proc.as_ptr(),
            libc::MS_NODEV | libc::MS_NOEXEC | libc::MS_NOSUID,
            ptr::null(),
        )
    })?)
}

fn mount_bin() -> io::Result<()> {
    let proc_mnt = CString::new("/bin")?;
    Ok(err_check(unsafe {
        libc::mount(
            proc_mnt.as_ptr(),
            proc_mnt.as_ptr(),
            ptr::null(),
            libc::MS_BIND | libc::MS_NOSUID,
            //libc::MS_NODEV | libc::MS_NOEXEC | libc::MS_NOSUID,
            ptr::null(),
        )
    })?)
}

fn visit_dirs(dir: &Path, cb: &dyn Fn(&std::fs::DirEntry) -> io::Result<()>) -> io::Result<()> {
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(&path, cb)?;
            } else {
                cb(&entry)?;
            }
        }
    }
    Ok(())
}

fn close_fds() -> io::Result<()> {
    let dir = Path::new("/proc/self/fd");
    let file = std::fs::File::open(dir)?;
    let dir_handle: RawFd = file.into_raw_fd();

    visit_dirs(Path::new("/proc/self/fd"), &|entry| {
        let path = entry.path();
        if let Some(num) = path.file_name() {
            //            println!("pre-fd: {:?}", num);
            if let Ok(fd) = i32::from_str_radix(num.to_str().unwrap(), 10) {
                if fd > 2 && fd != dir_handle {
                    if path.exists() {
                        println!("closing fd: {}", fd);
                        let _ = unsafe { std::fs::File::from_raw_fd(fd) };
                    }
                }
            }
        }
        Ok(())
    })?;
    Ok(())
}

fn spawn_sandbox<F>(mappings: Vec<Mapping>, sandboxed_func: F) -> io::Result<()>
where
    F: FnOnce(),
{
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    let (child_pid, _pid_fd) = clone3()?;
    let child_span = span!(Level::INFO, "sandbox_process");
    if child_pid == 0 {
        child_span.in_scope(|| {
            unsafe {
                // Kill with SIGKILL if Parent dies
                err_check(libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL))?;
            }

            info!("Starting setup: ");
            info!("\t Closing FDs...");
            //            close_fds()?;
            info!("\t User...");
            setup_user_namespace(uid, gid)?;
            info!("\t Mount...");
            setup_mount_namespace()?;
            //network_namespace()?;
            info!("\t Mount Sandbox...");
            mount_sandbox(Path::new("/home/ben/workspace/gaudi/sandbox"))?;
            info!("\t Create empty file...");
            create_empty_file()?;
            info!("\t Mount Dev...");
            mount_dev()?;
            info!("\t Mount Proc...");
            mount_proc()?;
            info!("\t Mount mounts...");
            mount_mounts(Path::new("/home/ben/workspace/gaudi/sandbox"), mappings)?;
            info!("\t Change Root...");
            change_root()?;
            sandboxed_func();
            std::process::exit(0);

            #[allow(unreachable_code)]
            Ok::<(), io::Error>(())
        })?;
    } else {
        info!("Spawned: {}", child_pid);
    }
    Ok(())
}
