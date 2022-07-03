use crate::{duplex, imp, Deserialize, FnOnce, Object, Receiver};
use nix::{
    libc::{c_void, pid_t},
    sys::signal,
};
use std::ffi::CString;
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

pub(crate) fn _spawn_child(child_fd: RawFd) -> Result<nix::unistd::Pid> {
    match unsafe {
        nix::libc::syscall(
            nix::libc::SYS_clone,
            nix::libc::SIGCHLD,
            std::ptr::null::<c_void>(),
        )
    } {
        -1 => Err(std::io::Error::last_os_error()),
        0 => {
            signal::sigprocmask(
                signal::SigmaskHow::SIG_SETMASK,
                Some(&signal::SigSet::empty()),
                None,
            )?;
            for i in 1..32 {
                if i != nix::libc::SIGKILL && i != nix::libc::SIGSTOP {
                    unsafe {
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
            }

            imp::disable_cloexec(child_fd)?;

            nix::unistd::execv(
                &CString::new("/proc/self/exe").unwrap(),
                &[
                    CString::new("_multiprocessing_").unwrap(),
                    CString::new(child_fd.to_string()).unwrap(),
                ],
            )
            .expect("execve failed");

            unreachable!();
        }
        child_pid => Ok(nix::unistd::Pid::from_raw(child_pid as pid_t)),
    }
}

pub fn spawn<T: Object>(entry: Box<dyn FnOnce<(RawFd,), Output = i32>>) -> Result<Child<T>> {
    let (mut local, child) = duplex::<Box<dyn FnOnce<(RawFd,), Output = i32>>, T>()?;

    let child_fd = child.as_raw_fd();

    let pid = _spawn_child(child_fd)?;

    local.send(&entry)?;
    Ok(Child::new(pid, local.into_receiver()))
}
