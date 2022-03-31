use anyhow::{bail, Context, Result};
use libc::{c_int, pid_t};
use std::marker::PhantomData;
use std::panic::UnwindSafe;
pub use tokio_fork::Child;

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

pub fn spawn_async<F: FnOnce() -> () + Send + UnwindSafe + 'static>(
    f: F,
) -> Result<tokio_fork::Child> {
    let fork_result = unsafe { tokio_fork::fork() }.with_context(|| "fork() failed")?;
    match fork_result {
        tokio_fork::Fork::Parent(child) => Ok(child),
        tokio_fork::Fork::Child => {
            let panic = std::panic::catch_unwind(f);
            let exit_code = if panic.is_ok() { 0 } else { 1 };
            unsafe {
                libc::_exit(exit_code);
            }
        }
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
    pub fn get_pid(&self) -> pid_t {
        self.pid
    }
}

impl Drop for JoinGuard<'_> {
    fn drop(&mut self) {
        self._join().unwrap();
    }
}

#[must_use = "process will be immediately joined if `AsyncJoinGuard` is not used"]
pub struct AsyncJoinGuard<'a> {
    child: tokio_fork::Child,
    joined: bool,
    _marker: PhantomData<&'a ()>,
}

pub fn scoped_async<'a, F: FnOnce() -> () + Send + UnwindSafe + 'a>(
    f: F,
) -> Result<AsyncJoinGuard<'a>> {
    let f: Box<dyn FnOnce() -> () + Send + UnwindSafe + 'a> = Box::new(f);
    let f: Box<dyn FnOnce() -> () + Send + UnwindSafe + 'static> =
        unsafe { std::mem::transmute(f) };
    Ok(AsyncJoinGuard {
        child: spawn_async(f)?,
        joined: false,
        _marker: PhantomData,
    })
}

impl AsyncJoinGuard<'_> {
    async fn _join(&mut self) -> Result<()> {
        if self.joined {
            return Ok(());
        }
        let exit_status = self.child.wait().await?;
        self.joined = true;
        if exit_status.success() {
            Ok(())
        } else {
            bail!("Process returned exit code {:?}", exit_status.code());
        }
    }
    pub async fn join(mut self) -> Result<()> {
        self._join().await
    }
    pub fn get_pid(&self) -> pid_t {
        self.child.pid()
    }
}

impl Drop for AsyncJoinGuard<'_> {
    fn drop(&mut self) {
        (JoinGuard {
            pid: self.child.pid(),
            joined: false,
            _marker: PhantomData,
        })
        .join()
        .unwrap()
    }
}
