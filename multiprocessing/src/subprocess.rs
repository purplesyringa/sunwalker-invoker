use crate::{Deserialize, Receiver};
use std::io::Result;

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
