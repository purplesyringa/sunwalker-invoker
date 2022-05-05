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
    if let Some(s) = args.next() {
        if s.starts_with("multiprocessing_") {
            let entry_points = ENTRY_POINTS
                .read()
                .expect("Failed to acquire read access to ENTRY_POINTS");
            if let Some(entry) = entry_points.get(s.as_str()) {
                let input_rx_fd: RawFd = args
                    .next()
                    .expect("Expected two CLI arguments for multiprocessing_*")
                    .parse()
                    .expect(
                        "Expected the first CLI argument after multiprocessing_* to be an integer",
                    );
                let output_tx_fd: RawFd = args
                    .next()
                    .expect("Expected two CLI arguments for multiprocessing_*")
                    .parse()
                    .expect(
                        "Expected the second CLI argument after multiprocessing_* to be an integer",
                    );
                enable_cloexec(input_rx_fd).expect("Failed to set O_CLOEXEC for input_rx_fd");
                enable_cloexec(output_tx_fd).expect("Failed to set O_CLOEXEC for output_tx_fd");
                std::process::exit(entry(input_rx_fd, output_tx_fd));
            }
        }
    }
    std::process::exit(MAIN_ENTRY
        .read()
        .expect("Failed to acquire read access to MAIN_ENTRY")
        .expect(
            "MAIN_ENTRY was not registered: is #[multiprocessing::main] missing?",
        )());
}

pub fn disable_cloexec(fd: RawFd) -> std::io::Result<()> {
    fcntl::fcntl(fd, fcntl::FcntlArg::F_SETFD(fcntl::FdFlag::empty()))?;
    Ok(())
}

pub fn enable_cloexec(fd: RawFd) -> std::io::Result<()> {
    fcntl::fcntl(fd, fcntl::FcntlArg::F_SETFD(fcntl::FdFlag::FD_CLOEXEC))?;
    Ok(())
}
