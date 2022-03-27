use anyhow::{bail, Context, Result};
use libc::{c_int, CLONE_NEWNS};
use nix::{fcntl, unistd};
use sunwalker_runner::{
    cgroups, corepool,
    image::{config, mount, package},
    system,
};

fn worker_main() -> Result<()> {
    unsafe {
        // Unshare namespaces
        if libc::unshare(CLONE_NEWNS) != 0 {
            bail!("Initial unshare() failed, unable to continue safely");
        }

        // Do not propagate mounts
        system::change_propagation("/", system::MS_PRIVATE | system::MS_REC)
            .with_context(|| "Setting propagation of / to private recursively failed")?;

        // Mount tmpfs
        system::mount("none", "/tmp", "tmpfs", 0, None)
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
            system::bind_mount(&source, &target)
                .with_context(|| format!("Bind-mounting {} -> {} failed", target, source))?;
        }
    }

    let cfg = config::Config::load(
        &std::fs::read_to_string("/home/ivanq/Documents/sunwalker/image.cfg")
            .with_context(|| "Could not read image.cfg")?,
    )
    .with_context(|| "Could not load image.cfg")?;

    let mut mnt = mount::ImageMounter::new();
    let mounted_image = mnt
        .mount("/home/ivanq/Documents/sunwalker/image.sfs", cfg)
        .with_context(|| "Could not mount image.sfs")?;

    let cores: Vec<u64> = vec![2, 3];
    cgroups::isolate_cores(&cores).with_context(|| "Failed to isolate CPU cores")?;

    let mut pool = corepool::CorePool::new(cores).with_context(|| "Could not create core pool")?;

    std::fs::write(
        "/tmp/hello-world.cpp",
        "#include <bits/stdc++.h>\nint main() {\n\tstd::cout << \"Hello, world!\" << std::endl;\n}\n",
    )?;

    for i in 0..50 {
        let mounted_image = &mounted_image;
        pool.spawn_dedicated(corepool::Task {
            callback: Box::new(move || {
                let package = mounted_image
                    .get_package("gcc")
                    .expect("Package gcc does not exist");

                let sandbox_config = package::SandboxConfig {
                    max_size_in_bytes: 1024 * 1024,
                    max_inodes: 10 * 1024,
                    bound_files: Vec::new(),
                };

                let lang = package
                    .get_language("c++.20.gcc")
                    .expect("Failed to get language c++.20.gcc");

                println!(
                    "{}",
                    lang.identify(&sandbox_config)
                        .expect("Failed to identify lang")
                );

                // let program = lang
                //     .build(vec!["/tmp/hello-world.cpp"], &sandbox_config)
                //     .expect("Failed to build /tmp/hello-world.cpp as c++.20.gcc");

                // lang.get_ready_to_run(&sandbox_config)
                //     .expect("Failed to get ready for running /tmp/hello-world.cpp as c++.20.gcc");

                // lang.run(&sandbox_config, &program)
                //     .expect("Failed to run /tmp/hello-world.cpp as c++.20.gcc");

                // lang.run(&sandbox_config, &program)
                //     .expect("Failed to run /tmp/hello-world.cpp as c++.20.gcc");

                // lang.run(&sandbox_config, &program)
                //     .expect("Failed to run /tmp/hello-world.cpp as c++.20.gcc");
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
    lisp::initialize();

    // Acquire a lock
    let lock_fd = fcntl::open(
        "/tmp/sunwalker_runner.lock",
        fcntl::OFlag::O_CREAT | fcntl::OFlag::O_RDWR,
        nix::sys::stat::Mode::from_bits(0o600).unwrap(),
    )
    .expect("Failed to open /tmp/sunwalker_runner.lock");

    fcntl::flock(lock_fd, fcntl::FlockArg::LockExclusiveNonblock)
        .expect("/tmp/sunwalker_runner.lock is already locked by another process (is sunwalker already running?)");

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
            .with_context(|| "/tmp/sunwalker_runner.lock could not be unlocked")
    }
}
