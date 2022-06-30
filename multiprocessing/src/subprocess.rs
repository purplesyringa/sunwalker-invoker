use crate::{duplex, imp, Deserialize, FnOnce, Object, Receiver};
use std::io::Result;
use std::os::unix::{
    io::{AsRawFd, RawFd},
    process::CommandExt,
};

pub struct Child<T: Deserialize> {
    proc: std::process::Child,
    output_rx: Receiver<T>,
}

impl<T: Deserialize> Child<T> {
    pub fn new(proc: std::process::Child, output_rx: Receiver<T>) -> Child<T> {
        Child { proc, output_rx }
    }

    pub fn kill(&mut self) -> Result<()> {
        self.proc.kill()
    }

    pub fn id(&mut self) -> u32 {
        self.proc.id()
    }

    pub fn join(&mut self) -> Result<T> {
        let value = self.output_rx.recv()?;
        if self.proc.wait()?.success() {
            value.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "The subprocess terminated without returning a value",
                )
            })
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "The subprocess did not terminate successfully",
            ))
        }
    }
}

pub fn spawn<T: Object>(entry: Box<dyn FnOnce<(RawFd,), Output = i32>>) -> Result<Child<T>> {
    let (mut local, child) = duplex::<Box<dyn FnOnce<(RawFd,), Output = i32>>, T>()?;

    let child_fd = child.as_raw_fd();

    let mut command = std::process::Command::new("/proc/self/exe");
    let child = unsafe {
        command
            .arg0("_multiprocessing_")
            .arg(child_fd.to_string())
            .pre_exec(move || {
                imp::disable_cloexec(child_fd)?;
                Ok(())
            })
            .spawn()?
    };

    local.send(&entry)?;

    Ok(Child::new(child, local.into_receiver()))
}
