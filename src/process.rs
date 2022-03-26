use anyhow::{bail, Context, Result};
use libc::{c_int, pid_t};
use std::marker::PhantomData;
use std::panic::UnwindSafe;

pub fn spawn<F: FnOnce() -> () + Send + UnwindSafe + 'static>(f: F) -> Result<pid_t> {
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

#[must_use = "process will be immediately joined if `JoinGuard` is not used"]
pub struct JoinGuard<'a> {
    pid: pid_t,
    joined: bool,
    _marker: PhantomData<&'a ()>,
}

pub fn scoped<'a, F: FnOnce() -> () + Send + UnwindSafe + 'a>(f: F) -> Result<JoinGuard<'a>> {
    let f: Box<dyn FnOnce() -> () + Send + UnwindSafe + 'a> = Box::new(f);
    let f: Box<dyn FnOnce() -> () + Send + UnwindSafe + 'static> =
        unsafe { std::mem::transmute(f) };
    Ok(JoinGuard {
        pid: spawn(f)?,
        joined: false,
        _marker: PhantomData,
    })
}

impl JoinGuard<'_> {
    fn _join(&mut self) -> Result<()> {
        if self.joined {
            return Ok(());
        }
        let mut wstatus: c_int = 0;
        let ret = unsafe { libc::waitpid(self.pid, &mut wstatus, 0) };
        if ret == -1 {
            Err(std::io::Error::last_os_error())
                .with_context(|| format!("waitpid() failed in JoinGuard::join()"))
        } else {
            self.joined = true;
            if libc::WIFEXITED(wstatus) {
                Ok(())
            } else {
                bail!("Process returned exit code {}", libc::WEXITSTATUS(wstatus));
            }
        }
    }
    pub fn join(mut self) -> Result<()> {
        self._join()
    }
}

impl Drop for JoinGuard<'_> {
    fn drop(&mut self) {
        self._join().unwrap();
    }
}
