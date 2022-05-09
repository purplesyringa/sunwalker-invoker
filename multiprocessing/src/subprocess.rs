use crate::{channel, imp, Await, Deserialize, FnOnce, Object, Receiver};
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

pub fn spawn<T: Object, U: Await<T>>(
    entry: Box<dyn FnOnce<(RawFd, RawFd), Output = i32>>,
    f: Box<dyn FnOnce<(), Output = U>>,
) -> Result<Child<T>> {
    let (mut entry_tx, entry_rx) = channel::<Box<dyn FnOnce<(RawFd, RawFd), Output = i32>>>()?;
    let entry_rx_fd = entry_rx.as_raw_fd();

    let (mut input_tx, input_rx) = channel::<Box<dyn FnOnce<(), Output = U>>>()?;
    let input_rx_fd = input_rx.as_raw_fd();

    let (output_tx, output_rx) = channel::<T>()?;
    let output_tx_fd = output_tx.as_raw_fd();

    let mut command = ::std::process::Command::new("/proc/self/exe");
    let child = unsafe {
        command
            .arg0("_multiprocessing_")
            .arg(entry_rx_fd.to_string())
            .arg(input_rx_fd.to_string())
            .arg(output_tx_fd.to_string())
            .pre_exec(move || {
                imp::disable_cloexec(entry_rx_fd)?;
                imp::disable_cloexec(input_rx_fd)?;
                imp::disable_cloexec(output_tx_fd)?;
                Ok(())
            })
            .spawn()?
    };

    entry_tx.send(&entry)?;
    input_tx.send(&f)?;

    Ok(::multiprocessing::Child::new(child, output_rx))
}
