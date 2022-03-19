use libc::{c_char, c_int, c_void, CLONE_NEWNS, CLONE_NEWUSER};
use nix::fcntl;
use sunwalker_runner::{image::mount, cgroups};


macro_rules! cstr {
    ($s:expr) => (
        concat!($s, "\0") as *const str as *const [c_char] as *const c_char
    );
}


// extern "C" fn worker_main(_: *mut c_void) -> i32 {
//     println!("bruh");
//     2
// }


// pub fn start_worker() {
//     let mut stack = vec! [0u8; 4096]; // 4 KB ought to be enough for anybody
//     let flags: c_int = 0; // todo: CLONE_VFORK
//     let pid = unsafe {
//         libc::clone(worker_main, stack.as_mut_ptr_range().end as *mut c_void, flags, std::ptr::null_mut())
//     };
//     dbg!(pid);
// }


fn worker_main() {
    unsafe {
        let euid = libc::geteuid();
        let egid = libc::getegid();

        // Unshare namespaces
        if libc::unshare(CLONE_NEWNS | CLONE_NEWUSER) != 0 {
            panic!("Initial unshare() failed, unable to continue safely");
        }

        // Fill uid/gid maps
        std::fs::write("/proc/self/uid_map", format!("0 {} 1\n", euid)).expect("Failed to write to uid_map");
        std::fs::write("/proc/self/setgroups", "deny\n").expect("Failed to write to setgroups");
        std::fs::write("/proc/self/gid_map", format!("0 {} 1\n", egid)).expect("Failed to write to gid_map");

        if libc::seteuid(0) != 0 {
            panic!("Unable to seteuid to 0");
        }
        if libc::setegid(0) != 0 {
            panic!("Unable to setegid to 0");
        }

        // Mount tmpfs
        if libc::mount(cstr!("tmpfs"), cstr!("/tmp"), cstr!("tmpfs"), 0, std::ptr::null()) != 0 {
            panic!("Mounting tmpfs on /tmp failed");
        }
    }


    let mut mnt = mount::ImageMounter::new();
    let _mounted_image = mnt.mount("/home/ivanq/Documents/sunwalker/image.sfs").expect("could not mount image.sfs");

    println!("Hi");
    std::thread::sleep_ms(1000);
    // start_worker();
}


fn watchdog_main(worker_pid: libc::pid_t) {
    println!("Watchdog started for {}", worker_pid);

    let mut status: c_int = 0;
    if unsafe { libc::waitpid(worker_pid, &mut status as *mut c_int, libc::WUNTRACED) } != worker_pid {
        panic!("waitpid() failed when waiting for the worker to stop at bootstrap");
    }

    // Initialization
    cgroups::create_root_cpuset().expect("Creating root cpuset failed");

    // CONT worker
    if unsafe { libc::kill(worker_pid, libc::SIGCONT) } == -1 {
        panic!("Failed to send SIGCONT to the worker at bootstrap");
    }

    // Wait for the worker to stop
    if unsafe { libc::waitpid(worker_pid, &mut status as *mut c_int, 0) } != worker_pid {
        panic!("waitpid() failed when waiting for an event from worker");
    }

    // TODO: handle exit status here and clean up garbage
}


fn main() {
    // Acquire a lock
    let lock_fd = fcntl::open("/var/run/sunwalker_runner.lock", fcntl::OFlag::O_CREAT | fcntl::OFlag::O_RDWR, nix::sys::stat::Mode::from_bits(0o700).unwrap())
        .expect("Failed to open /var/run/sunwalker_runner.lock");
    fcntl::flock(lock_fd, fcntl::FlockArg::LockExclusiveNonblock)
        .expect("/var/run/sunwalker_runner.lock is already locked by another process (is sunwalker already running?)");

    // Spawn a watchdog
    let child_pid = unsafe { libc::fork() };
    if child_pid == -1 {
        panic!("Starting a watchdog via fork() failed");
    } else if child_pid == 0 {
        // Pause ourselves; watchdog will CONT us when it's ready
        unsafe {
            libc::raise(libc::SIGSTOP);
        }
        worker_main()
    } else {
        watchdog_main(child_pid)
    }
}
