use std::arch::asm;

use libc::pid_t;

#[repr(C)]
struct clone_args {
    flags: u64,
    pidfd: u64,
    child_tid: u64,
    parent_tid: u64,
    exit_signal: u64,
    stack: u64,
    stack_size: u64,
    tls: u64,
    set_tid: u64,
    set_tid_size: u64,
    cgroup: u64,
}

pub(crate) fn clone3() -> std::io::Result<(pid_t, pid_t)> {
    let mut pidfd = -1;

    let mut args = clone_args {
        flags: (libc::CLONE_NEWUSER
            | libc::CLONE_NEWNET
            | libc::CLONE_NEWUTS
            | libc::CLONE_NEWNS
            | libc::CLONE_NEWIPC
            | libc::CLONE_NEWPID
            | libc::CLONE_PIDFD) as u64,
        pidfd: &mut pidfd as *mut libc::pid_t as u64,
        child_tid: 0,
        parent_tid: 0,
        exit_signal: libc::SIGCHLD as u64,
        stack: 0,
        stack_size: 0,
        tls: 0,
        set_tid: 0,
        set_tid_size: 0,
        cgroup: 0,
    };

    let args_ptr = &mut args as *mut clone_args as usize;
    let args_size = std::mem::size_of::<clone_args>();

    let ret = unsafe { syscall_2(libc::SYS_clone3 as usize, args_ptr, args_size) };

    if ret == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok((ret as pid_t, pidfd))
    }
}

pub unsafe fn syscall_0(n: usize) -> i32 {
    let ret: i32;
    asm!(
        "syscall",
        in("rax") n,
        out("rcx") _,
        out("r11") _,
        lateout("rax") ret,
    );
    ret
}

pub unsafe fn syscall_1(n: usize, arg1: usize) -> i32 {
    let ret: i32;
    asm!(
        "syscall",
        in("rax") n,
        in("rdi") arg1,
        out("rcx") _,
        out("r11") _,
        lateout("rax") ret,
    );
    ret
}

pub unsafe fn syscall_2(n: usize, arg1: usize, arg2: usize) -> i32 {
    let ret: i32;
    asm!(
        "syscall",
        in("rax") n,
        in("rdi") arg1,
        in("rsi") arg2,
        out("rcx") _,
        out("r11") _,
        lateout("rax") ret,
    );
    ret
}

pub unsafe fn syscall_3(n: usize, arg1: usize, arg2: usize, arg3: usize) -> i32 {
    let ret: i32;
    asm!(
        "syscall",
        in("rax") n,
        in("rdi") arg1,
        in("rsi") arg2,
        in("rdx") arg3,
        out("rcx") _,
        out("r11") _,
        lateout("rax") ret,
    );
    ret
}

pub unsafe fn syscall_4(n: usize, arg1: usize, arg2: usize, arg3: usize, arg4: usize) -> i32 {
    let ret: i32;
    asm!(
        "syscall",
        in("rax") n,
        in("rdi") arg1,
        in("rsi") arg2,
        in("rdx") arg3,
        in("r10") arg4,
        out("rcx") _,
        out("r11") _,
        lateout("rax") ret,
    );
    ret
}

pub unsafe fn syscall_5(
    n: usize,
    arg1: usize,
    arg2: usize,
    arg3: usize,
    arg4: usize,
    arg5: usize,
) -> i32 {
    let ret: i32;
    asm!(
        "syscall",
        in("rax") n,
        in("rdi") arg1,
        in("rsi") arg2,
        in("rdx") arg3,
        in("r10") arg4,
        in("r8") arg5,
        out("rcx") _,
        out("r11") _,
        lateout("rax") ret,
    );
    ret
}

pub unsafe fn syscall_6(
    n: usize,
    arg1: usize,
    arg2: usize,
    arg3: usize,
    arg4: usize,
    arg5: usize,
    arg6: usize,
) -> i32 {
    let ret: i32;
    asm!(
        "syscall",
        in("rax") n,
        in("rdi") arg1,
        in("rsi") arg2,
        in("rdx") arg3,
        in("r10") arg4,
        in("r8") arg5,
        in("r9") arg6,
        out("rcx") _,
        out("r11") _,
        lateout("rax") ret,
    );
    ret
}
