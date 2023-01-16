use crate::action::platform::linux::{clone3, syscall_2};
use std::{
    ffi::CString,
    io::{self, Write},
    os::fd::RawFd,
    path::{Path, PathBuf},
    ptr,
};
use tokio::io::unix::AsyncFd;
use tracing::{info, instrument, span, Level};

#[derive(Clone, Debug)]
pub struct Mapping {
    pub dest_path: PathBuf,
    pub source_path: PathBuf,
}

pub struct AsyncSandboxedAction {
    inner: AsyncFd<RawFd>,
}

impl AsyncSandboxedAction {
    pub async fn status(&self) -> io::Result<()> {
        loop {
            let mut guard = self.inner.readable().await?;

            match guard.try_io(|inner| {
                info!("Got a pidfd update");

                Ok(())
            }) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SandboxedAction {
    program: String,
    sandbox_location: PathBuf,
    arguments: Vec<String>,
    environment: Vec<(String, String)>,
    output_files: Vec<PathBuf>,
    input_files: Vec<Mapping>,
}

impl SandboxedAction {
    pub fn new(program: &str) -> Self {
        SandboxedAction {
            program: program.into(),
            sandbox_location: PathBuf::from("/home/ben/workspace/gaudi/sandbox"),
            ..Default::default()
        }
    }

    pub fn args(mut self, args: &[String]) -> Self {
        self.arguments.extend_from_slice(args);
        self
    }

    pub fn envs(mut self, env: &[(String, String)]) -> Self {
        self.environment.extend_from_slice(env);
        self
    }

    pub fn output_files(mut self, output_files: &[PathBuf]) -> Self {
        self.output_files.extend_from_slice(output_files);
        self
    }

    pub fn input_file_mapping(mut self, mapping: &[Mapping]) -> Self {
        self.input_files.extend_from_slice(mapping);
        self
    }

    pub fn input_file(mut self, path: &str) -> Self {
        let source_path = PathBuf::from(path);
        self.input_files.push(Mapping {
            dest_path: PathBuf::from(format!("{}/{}", self.sandbox_location.display(), path)),
            source_path,
        });
        self
    }

    #[instrument(skip_all)]
    pub fn spawn(&mut self) -> io::Result<AsyncSandboxedAction> {
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let (child_pid, pid_fd) = clone3()?;
        let child_span = span!(Level::INFO, "sandbox_process");
        if child_pid == 0 {
            unsafe {
                // Kill with SIGKILL if Parent dies
                err_check(libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL))?;
            }
            child_span.in_scope(|| {
                info!("Setting up spandboxed process...");

                info!("Setting up User namespace...");
                setup_user_namespace(uid, gid)?;

                info!("Setting up Mount namespace...");
                setup_mount_namespace()?;

                info!("Mounting Sandbox...");
                mount_sandbox(Path::new("/home/ben/workspace/gaudi/sandbox"))?;
                mount_dev()?;

                info!("Mounting Proc...");
                mount_proc()?;

                info!("Mounting all input files...");
                mount_mounts(
                    Path::new("/home/ben/workspace/gaudi/sandbox"),
                    &self.input_files,
                )?;
                info!("Change Root...");
                change_root()?;

                unsafe {
                    let c = CString::new(self.program.as_str())?;
                    info!("cmd: {:?}", c);
                    let args = self
                        .arguments
                        .iter()
                        .map(|arg| CString::new(arg.as_str()).unwrap())
                        .collect::<Vec<CString>>();
                    info!("args: {:?}", args);
                    let mut argv = args
                        .iter()
                        .map(|arg| arg.as_ptr())
                        .collect::<Vec<*const libc::c_char>>();
                    argv.push(std::ptr::null());

                    let env = self
                        .environment
                        .iter()
                        .map(|arg| format!("{}={}", arg.0, arg.1))
                        .map(|arg| CString::new(arg.as_str()).unwrap())
                        .collect::<Vec<CString>>();
                    info!("env: {:?}", env);
                    let mut envv = env
                        .iter()
                        .map(|arg| arg.as_ptr())
                        .collect::<Vec<*const libc::c_char>>();
                    envv.push(std::ptr::null());

                    // Create directories for all output files
                    for output in self.output_files.iter() {
                        if let Some(prefix) = output.parent() {
                            std::fs::create_dir_all(prefix).unwrap();
                        }
                    }
                    libc::setpgid(0, 0);
                    libc::umask(022);

                    err_check(libc::execvpe(c.as_ptr(), argv.as_ptr(), envv.as_ptr()))?;
                }

                // just used for type annotations, execvp/pidfd
                // changed how this whole thing returns
                unreachable!();
                #[allow(unreachable_code)]
                Err(io::Error::new(io::ErrorKind::Other, ""))
            })?
        } else {
            let inner = AsyncFd::new(pid_fd)?;
            Ok(AsyncSandboxedAction { inner })
        }
    }
}

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

fn mount_mounts(path: &Path, mount_mapping: &[Mapping]) -> io::Result<()> {
    for mount in mount_mapping {
        info!(
            "Binding {} -> {}",
            mount.source_path.display(),
            mount.dest_path.display()
        );

        // create a file to bind against at the depth
        if mount.source_path.is_file() {
            if let Some(prefix) = mount.dest_path.parent() {
                std::fs::create_dir_all(prefix)?;
            }
            std::fs::File::create(&mount.dest_path)?;
        } else {
            std::fs::create_dir_all(&mount.dest_path)?;
        }

        let src = path_to_cstring(&mount.source_path).unwrap();
        let target = path_to_cstring(&mount.dest_path).unwrap();
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

pub fn path_to_cstring<P: AsRef<Path>>(path: &P) -> Option<CString> {
    path.as_ref()
        .to_str()
        .and_then(|p| std::ffi::CString::new(p).ok())
}

/// Turn the unix error codes into something more Rust-y
fn err_check(ret: i32) -> io::Result<()> {
    if ret == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
