pub use ctor::ctor;

use lazy_static::lazy_static;
use nix::fcntl;
use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::RwLock;

lazy_static! {
    pub static ref MAIN_ENTRY: RwLock<Option<fn() -> i32>> = RwLock::new(None);
    pub static ref ENTRY_POINTS: RwLock<HashMap<&'static str, fn(RawFd, RawFd) -> i32>> =
        RwLock::new(HashMap::new());
}

pub trait Report {
    fn report(self) -> i32;
}

impl Report for () {
    fn report(self) -> i32 {
        0
    }
}

impl<T, E: std::fmt::Debug> Report for Result<T, E> {
    fn report(self) -> i32 {
        match self {
            Ok(_) => 0,
            Err(e) => {
                eprintln!("Error: {e:?}");
                1
            }
        }
    }
}

pub fn main() {
    let mut args = std::env::args();
    let s = args.next().unwrap();
    if s.starts_with("multiprocessing_") {
        let entry_points = ENTRY_POINTS.read().unwrap();
        if let Some(entry) = entry_points.get(s.as_str()) {
            let input_rx_fd: RawFd = args.next().unwrap().parse().unwrap();
            let output_tx_fd: RawFd = args.next().unwrap().parse().unwrap();
            enable_cloexec(input_rx_fd).unwrap();
            enable_cloexec(output_tx_fd).unwrap();
            std::process::exit(entry(input_rx_fd, output_tx_fd));
        }
    }
    std::process::exit(MAIN_ENTRY.read().unwrap().unwrap()());
}

pub fn disable_cloexec(fd: RawFd) -> std::io::Result<()> {
    fcntl::fcntl(fd, fcntl::FcntlArg::F_SETFD(fcntl::FdFlag::empty()))?;
    Ok(())
}

pub fn enable_cloexec(fd: RawFd) -> std::io::Result<()> {
    fcntl::fcntl(fd, fcntl::FcntlArg::F_SETFD(fcntl::FdFlag::FD_CLOEXEC))?;
    Ok(())
}
