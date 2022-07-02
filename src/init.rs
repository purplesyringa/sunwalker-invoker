use crate::{cgroups, client};
use anyhow::{bail, Context, Result};
use clap::Parser;
use libc::c_int;
use nix::{fcntl, unistd};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct CLIArgs {
    #[clap(short, long)]
    pub config: String,
}

fn watchdog_main(invoker_pid: libc::pid_t) -> Result<()> {
    println!("Watchdog started for {}", invoker_pid);

    let mut status: c_int = 0;
    if unsafe { libc::waitpid(invoker_pid, &mut status as *mut c_int, libc::WUNTRACED) }
        != invoker_pid
    {
        bail!("waitpid() failed when waiting for the invoker to stop at bootstrap");
    }

    // Initialization
    cgroups::drop_existing_affine_cpusets()
        .with_context(|| "Failed to remove dangling cpusets at boot")?;
    cgroups::create_root_cpuset().with_context(|| "Failed to create root cpuset")?;

    // CONT invoker
    if unsafe { libc::kill(invoker_pid, libc::SIGCONT) } == -1 {
        bail!("Failed to send SIGCONT to the invoker at bootstrap");
    }

    // Wait for the invoker to stop
    if unsafe { libc::waitpid(invoker_pid, &mut status as *mut c_int, 0) } != invoker_pid {
        bail!("waitpid() failed when waiting for an event from invoker");
    }

    // TODO: handle exit status here

    // Garbage cleanup
    cgroups::drop_existing_affine_cpusets()
        .with_context(|| "Failed to remove dangling cpusets at shutdown")?;
    cgroups::isolate_cores(&vec![]).with_context(|| "Failed to revert CPU isolation")?;

    Ok(())
}

pub fn main() -> Result<()> {
    lisp::initialize();

    let cli_parse = CLIArgs::parse();

    std::fs::create_dir_all("/tmp/sunwalker_invoker")
        .expect("Failed to create /tmp/sunwalker_invoker directory");

    // Acquire a lock
    let lock_fd = fcntl::open(
        "/tmp/sunwalker_invoker/invoker.lock",
        fcntl::OFlag::O_CREAT | fcntl::OFlag::O_RDWR,
        nix::sys::stat::Mode::from_bits(0o600).unwrap(),
    )
    .expect("Failed to open /tmp/sunwalker_invoker/invoker.lock");

    fcntl::flock(lock_fd, fcntl::FlockArg::LockExclusiveNonblock).expect(
        "/tmp/sunwalker_invoker/invoker.lock is already locked by another process (is sunwalker \
         already running?)",
    );

    // Spawn a watchdog
    let child_pid = unsafe { libc::fork() };
    if child_pid == -1 {
        panic!("Starting a watchdog via fork() failed");
    } else if child_pid == 0 {
        unistd::close(lock_fd).expect("Failed to close lock fd");

        // Pause ourselves; watchdog will CONT us when it's ready
        unsafe {
            libc::raise(libc::SIGSTOP);
        }

        client::client_main(cli_parse)
    } else {
        watchdog_main(child_pid)?;
        fcntl::flock(lock_fd, fcntl::FlockArg::Unlock)
            .with_context(|| "Failed to unlock /tmp/sunwalker_invoker/invoker.lock")
    }
}
