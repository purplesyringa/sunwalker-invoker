use anyhow::{bail, Context, Result};
use libc::{c_int, CLONE_NEWNS, CLONE_NEWUSER, MS_PRIVATE, MS_REC};
use nix::{fcntl, unistd};
use sunwalker_runner::{
    cgroups, corepool,
    image::{mount, package},
};

fn worker_main() -> Result<()> {
    unsafe {
        // let euid = libc::geteuid();
        // let egid = libc::getegid();

        // Unshare namespaces
        if libc::unshare(CLONE_NEWNS /*| CLONE_NEWUSER*/) != 0 {
            bail!("Initial unshare() failed, unable to continue safely");
        }

        // Do not propagate mounts
        sys_mount::Mount::builder()
            .fstype("none")
            .flags(sys_mount::MountFlags::from_bits_unchecked(
                MS_PRIVATE | MS_REC,
            ))
            .mount("none", "/")
            .with_context(|| "Setting propagation of / to private recursively failed")?;

        // // Fill uid/gid maps
        // std::fs::write("/proc/self/uid_map", format!("0 {} 1\n", euid))
        //     .expect("Failed to write to uid_map");
        // std::fs::write("/proc/self/setgroups", "deny\n").expect("Failed to write to setgroups");
        // std::fs::write("/proc/self/gid_map", format!("0 {} 1\n", egid))
        //     .expect("Failed to write to gid_map");

        // if libc::seteuid(0) != 0 {
        //     panic!("Unable to seteuid to 0");
        // }
        // if libc::setegid(0) != 0 {
        //     panic!("Unable to setegid to 0");
        // }

        // Mount tmpfs
        sys_mount::Mount::builder()
            .fstype("tmpfs")
            .mount("tmpfs", "/tmp")
            .with_context(|| "Mounting tmpfs on /tmp failed")?;

        // Make various temporary directories
        std::fs::create_dir("/tmp/worker").with_context(|| "Creating /tmp/worker failed")?;

        // Prepare a copy of /dev
        std::fs::create_dir("/tmp/dev").with_context(|| "Creating /tmp/dev failed")?;
        for name in [
            "null", "full", "zero", "urandom", "random", "stdin", "stdout", "stderr", "shm",
            "mqueue", "ptmx", "pts", "fd",
        ] {
            let source = format!("/dev/{}", name);
            let target = format!("/tmp/dev/{}", name);
            let metadata = std::fs::symlink_metadata(&source)
                .with_context(|| format!("{} does not exist (or oculd not be accessed)", source))?;
            if metadata.is_symlink() {
                let symlink_target = std::fs::read_link(&source)
                    .with_context(|| format!("Cannot readlink {:?}", source))?;
                std::os::unix::fs::symlink(&symlink_target, &target)
                    .with_context(|| format!("Cannot ln -s {:?} {:?}", symlink_target, target))?;
                continue;
            } else if metadata.is_dir() {
                std::fs::create_dir(&target)
                    .with_context(|| format!("Cannot mkdir {:?}", target))?;
            } else {
                std::fs::File::create(&target)
                    .with_context(|| format!("Cannot touch {:?}", target))?;
            }
            sys_mount::Mount::builder()
                .flags(sys_mount::MountFlags::BIND)
                .mount(&source, &target)
                .with_context(|| format!("Bind-mounting {} -> {} failed", target, source))?;
        }
    }

    let mut mnt = mount::ImageMounter::new();
    let mounted_image = mnt
        .mount("/mnt/wwn-0x500000e041b68f2b-part1/sunwalker/image.sfs")
        .with_context(|| "Could not mount image.sfs")?;

    let cores: Vec<u64> = vec![4, 5, 6, 7];
    cgroups::isolate_cores(&cores).with_context(|| "Failed to isolate CPU cores")?;

    let mut pool = corepool::CorePool::new(cores).with_context(|| "Could not create core pool")?;

    std::fs::write(
        "/tmp/hello-world.cpp",
        "#include <iostream>\nint main() {\n\tstd::cout << \"Hello, world!\" << std::endl;\n}\n",
    )?;

    for i in 0..50 {
        let mounted_image = &mounted_image;
        pool.spawn_dedicated(corepool::Task {
            callback: Box::new(move || {
                let package = mounted_image
                    .get_package("gcc")
                    .expect("Package gcc does not exist");

                package
                    .enter(
                        &package::SandboxDiskConfig {
                            max_size_in_bytes: 1024 * 1024,
                            max_inodes: 10 * 1024,
                        }
                    )
                    .expect("Entering the package failed");

                std::fs::write(
                    "/test.cpp",
                    "#include <iostream>\nint main() {\n\tstd::cout << \"Hello, world!\" << std::endl;\n}\n",
                ).expect("write failed");

                std::process::Command::new("g++")
                    .arg("/test.cpp")
                    .arg("-o")
                    .arg("/test")
                    .output()
                    .expect("Spawning gcc failed");

                // println!("Hello, world from #{}!", i);
            }),
            group: String::from("default"),
        })
        .unwrap();
    }

    pool.join();

    println!("Spawned all");

    // std::thread::sleep_ms(100000);
    // start_worker();

    Ok(())
}

fn watchdog_main(worker_pid: libc::pid_t) -> Result<()> {
    println!("Watchdog started for {}", worker_pid);

    let mut status: c_int = 0;
    if unsafe { libc::waitpid(worker_pid, &mut status as *mut c_int, libc::WUNTRACED) }
        != worker_pid
    {
        bail!("waitpid() failed when waiting for the worker to stop at bootstrap");
    }

    // Initialization
    cgroups::drop_existing_affine_cpusets()
        .with_context(|| "Failed to remove dangling cpusets at boot")?;
    cgroups::create_root_cpuset().with_context(|| "Failed to create root cpuset")?;

    // CONT worker
    if unsafe { libc::kill(worker_pid, libc::SIGCONT) } == -1 {
        bail!("Failed to send SIGCONT to the worker at bootstrap");
    }

    // Wait for the worker to stop
    if unsafe { libc::waitpid(worker_pid, &mut status as *mut c_int, 0) } != worker_pid {
        bail!("waitpid() failed when waiting for an event from worker");
    }

    // TODO: handle exit status here

    // Garbage cleanup
    cgroups::drop_existing_affine_cpusets()
        .with_context(|| "Failed to remove dangling cpusets at shutdown")?;
    cgroups::isolate_cores(&vec![]).with_context(|| "Failed to revert CPU isolation")?;

    Ok(())
}

fn main() -> Result<()> {
    // Acquire a lock
    let lock_fd = fcntl::open(
        "/var/run/sunwalker_runner.lock",
        fcntl::OFlag::O_CREAT | fcntl::OFlag::O_RDWR,
        nix::sys::stat::Mode::from_bits(0o600).unwrap(),
    )
    .expect("Failed to open /var/run/sunwalker_runner.lock");

    fcntl::flock(lock_fd, fcntl::FlockArg::LockExclusiveNonblock)
        .expect("/var/run/sunwalker_runner.lock is already locked by another process (is sunwalker already running?)");

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
        worker_main()
    } else {
        watchdog_main(child_pid)?;
        fcntl::flock(lock_fd, fcntl::FlockArg::Unlock)
            .with_context(|| "/var/run/sunwalker_runner.lock could not be unlocked")
    }
}
