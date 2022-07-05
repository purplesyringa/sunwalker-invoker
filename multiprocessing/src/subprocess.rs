use crate::{caching, duplex, Deserialize, FnOnce, Object, Receiver};
use nix::{
    libc::{c_int, c_void, pid_t},
    sys::signal,
};
use std::io::Result;
use std::os::unix::io::{AsRawFd, RawFd};

pub struct Child<T: Deserialize> {
    proc_pid: nix::unistd::Pid,
    output_rx: Receiver<T>,
}

impl<T: Deserialize> Child<T> {
    pub fn new(proc_pid: nix::unistd::Pid, output_rx: Receiver<T>) -> Child<T> {
        Child {
            proc_pid,
            output_rx,
        }
    }

    pub fn kill(&mut self) -> Result<()> {
        signal::kill(self.proc_pid, signal::Signal::SIGKILL)?;
        Ok(())
    }

    pub fn id(&mut self) -> pid_t {
        self.proc_pid.into()
    }

    pub fn join(&mut self) -> Result<T> {
        let value = self.output_rx.recv()?;
        let status = nix::sys::wait::waitpid(self.proc_pid, None)?;
        if let nix::sys::wait::WaitStatus::Exited(_, 0) = status {
            value.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "The subprocess terminated without returning a value",
                )
            })
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "The subprocess did not terminate successfully: {:?}",
                    status
                ),
            ))
        }
    }
}

pub(crate) unsafe fn _spawn_child(child_fd: RawFd, flags: c_int) -> Result<nix::unistd::Pid> {
    match nix::libc::syscall(
        nix::libc::SYS_clone,
        nix::libc::SIGCHLD | flags,
        std::ptr::null::<c_void>(),
    ) {
        -1 => Err(std::io::Error::last_os_error()),
        0 => {
            signal::sigprocmask(
                signal::SigmaskHow::SIG_SETMASK,
                Some(&signal::SigSet::empty()),
                None,
            )?;
            for i in 1..32 {
                if i != nix::libc::SIGKILL && i != nix::libc::SIGSTOP {
                    signal::sigaction(
                        signal::Signal::try_from(i).unwrap(),
                        &signal::SigAction::new(
                            signal::SigHandler::SigDfl,
                            signal::SaFlags::empty(),
                            signal::SigSet::empty(),
                        ),
                    )?;
                }
            }

            // The code below is a less straightforward but much faster alternative to:
            // imp::disable_cloexec(child_fd)?;
            // nix::unistd::execv(
            //     &CString::new("/proc/self/exe").unwrap(),
            //     &[
            //         CString::new("_multiprocessing_").unwrap(),
            //         CString::new(child_fd.to_string()).unwrap(),
            //     ],
            // )
            // .expect("execve failed");

            // Close O_CLOEXEC descriptors except the ones we want to retain manually
            let mut fds_to_close = Vec::new();
            for entry in
                std::fs::read_dir("/proc/self/fd").expect("Failed to opendir /proc/self/fd")
            {
                let entry = entry.expect("Failed to readdir /proc/self/fd");
                let fd: RawFd = entry
                    .file_name()
                    .into_string()
                    .expect("A file in /proc/self/fd does not have a legible name")
                    .parse()
                    .expect("A file in /proc/self/fd does not have a digital name");
                if fd == child_fd || caching::is_retained_fd(fd) {
                    continue;
                }
                let fd_flags = nix::fcntl::fcntl(fd, nix::fcntl::FcntlArg::F_GETFD)
                    .expect("A file in /proc/self/fd does not designate a file descriptor");
                if fd_flags & nix::libc::FD_CLOEXEC != 0 {
                    // If we close all fds while reading the directory, we'll close its own fd too
                    fds_to_close.push(fd);
                }
            }
            for fd in fds_to_close.into_iter() {
                // close(2) can only reasonably fail on EBADF, which is fine if it happens. In fact,
                // it will happen, because the file descriptor that read_dir created is listed in
                // fds_to_close.
                let _ = nix::unistd::close(fd);
            }

            caching::restore(child_fd);
        }
        child_pid => Ok(nix::unistd::Pid::from_raw(child_pid as pid_t)),
    }
}

pub unsafe fn spawn<T: Object>(
    entry: Box<dyn FnOnce<(RawFd,), Output = i32>>,
    flags: c_int,
) -> Result<Child<T>> {
    let (mut local, child) = duplex::<Box<dyn FnOnce<(RawFd,), Output = i32>>, T>()?;

    let child_fd = child.as_raw_fd();

    let pid = _spawn_child(child_fd, flags)?;

    local.send(&entry)?;
    Ok(Child::new(pid, local.into_receiver()))
}
