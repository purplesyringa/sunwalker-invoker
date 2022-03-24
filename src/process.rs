use anyhow::{bail, Result};
use libc::pid_t;
use std::panic::UnwindSafe;

pub fn spawn<F: FnOnce() -> () + Sync + Send + UnwindSafe + 'static>(f: F) -> Result<pid_t> {
    let child_pid = unsafe { libc::fork() };
    if child_pid == -1 {
        bail!("fork() failed");
    } else if child_pid == 0 {
        let panic = std::panic::catch_unwind(f);
        let exit_code = if panic.is_ok() { 0 } else { 1 };
        unsafe {
            libc::_exit(exit_code);
        }
    } else {
        Ok(child_pid)
    }
}
