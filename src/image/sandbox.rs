use crate::{
    cgroups, errors,
    errors::{ToError, ToResult},
    image::package,
    system,
};
use futures_util::TryStreamExt;
use libc::{
    c_char, CLONE_NEWIPC, CLONE_NEWNET, CLONE_NEWNS, CLONE_NEWPID, CLONE_NEWUSER, CLONE_NEWUTS,
    CLONE_SYSVSEM,
};
use multiprocessing::Object;
use std::io::BufRead;
use std::os::unix::{
    fs::{MetadataExt, PermissionsExt},
    io::AsRawFd,
};
use std::path::PathBuf;

pub struct DiskQuotas {
    pub space: u64,
    pub max_inodes: u64,
}

pub struct RootFS {
    removed: bool,
    pub id: String,
    bound_files: Vec<(PathBuf, String)>,
    quotas: DiskQuotas,
}

pub struct Namespace {
    removed: bool,
    pub id: String,
}

// Unmount everything beneath prefix recursively. Does not unmount prefix itself.
fn unmount_recursively(prefix: &str, inclusive: bool) -> Result<(), errors::Error> {
    let prefix_slash = format!("{prefix}/");

    let file = std::fs::File::open("/proc/self/mounts")
        .context_invoker("Failed to open /proc/self/mounts for reading")?;

    let mut vec = Vec::new();
    for line in std::io::BufReader::new(file).lines() {
        let line = line.context_invoker("Failed to read /proc/self/mounts")?;
        let mut it = line.split(" ");
        it.next()
            .context_invoker("Invalid format of /proc/self/mounts")?;
        let target_path = it
            .next()
            .context_invoker("Invalid format of /proc/self/mounts")?;
        if target_path.starts_with(&prefix_slash) || (inclusive && target_path == prefix) {
            vec.push(target_path.to_string());
        }
    }

    for path in vec.into_iter().rev() {
        system::umount(&path).with_context_invoker(|| format!("Failed to unmount {path}"))?;
    }

    Ok(())
}

pub fn enter_worker_space(core: u64) -> Result<(), errors::Error> {
    // Unshare namespaces
    unsafe {
        if libc::unshare(CLONE_NEWNS) != 0 {
            return Err(std::io::Error::last_os_error()
                .context_invoker("Failed to unshare mount namespace"));
        }
    }

    // Create per-worker tmpfs
    system::mount("none", "/tmp/sunwalker_invoker/worker", "tmpfs", 0, None)
        .context_invoker("Failed to mount tmpfs on /tmp/sunwalker_invoker/worker")?;

    std::fs::create_dir("/tmp/sunwalker_invoker/worker/rootfs")
        .context_invoker("Failed to create /tmp/sunwalker_invoker/worker/rootfs")?;
    std::fs::create_dir("/tmp/sunwalker_invoker/worker/ns")
        .context_invoker("Failed to create /tmp/sunwalker_invoker/worker/ns")?;
    std::fs::create_dir("/tmp/sunwalker_invoker/worker/aux")
        .context_invoker("Failed to create /tmp/sunwalker_invoker/worker/aux")?;

    // Switch to core
    let pid = unsafe { libc::getpid() };
    cgroups::add_task_to_core(pid, core).with_context_invoker(|| {
        format!("Failed to move current process (PID {pid}) to core {core}")
    })?;

    Ok(())
}

pub fn make_rootfs(
    package: &package::Package,
    bound_files: Vec<(PathBuf, String)>,
    quotas: DiskQuotas,
    id: String,
) -> Result<RootFS, errors::Error> {
    // There are two (obvious) ways to mount an image in a writable way.
    //
    // First, we can mount a tmpfs that would store the ephemeral data, and then mount an overlayfs
    // that uses the image as the lowerdir and the ephemeral tmpfs as the upperdir (and the workdir
    // too).
    //
    // Second, we can mount a tmpfs on top of every directory that needs to be writable.
    //
    // The former way is, perhaps, more universal, but it has a slight defect in terms of
    // efficiency. Keep in mind that we have to somehow reset this structure every time we judge the
    // submission on a new test. Unfortunately, it's not as simple as cleaning the upperdir, due to
    // some caching shenanigans. Instead, we would have to somehow guess as to which files are from
    // the image and which are ephemeral and remove the latter explicitly. This is error-prone. To
    // add insult to injury, even this sort of clean-up may lead to information leak, because in
    // newer Linux kernels inodes are assigned consecutively, and removing a file does not free up
    // its inode number.
    //
    // The latter way, on the other hand, amounts to mounting an overlayfs with two lowerdirs--one
    // right from the image and one that contains two empty directories, /space and /dev. We would
    // then mount tmpfs on top of /space and /dev. To reset the rootfs, we would simply remount
    // /space. There would, of course, be some problems regarding dynamic content, which will have
    // to be remounted every time because it's located in /space too. Also, while using an overlayfs
    // to add a single directory to the tree seems suboptimal, it's perhaps fine, especially if
    // /space and /dev are in the *second* lowerdir, so that the tmpfs doesn't have to handle all
    // the accesses to the permanent files just to return ENOENT.

    let prefix = format!("/tmp/sunwalker_invoker/worker/rootfs/{id}");

    std::fs::create_dir(&prefix).context_invoker("Failed to create directory <prefix>")?;

    if let Err(e) = try {
        std::fs::create_dir(format!("{prefix}/ephemeral"))
            .context_invoker("Failed to create directory <prefix>/ephemeral")?;
        std::fs::create_dir(format!("{prefix}/overlay"))
            .context_invoker("Failed to create directory <prefix>/overlay")?;
        std::fs::create_dir(format!("{prefix}/overlay/root"))
            .context_invoker("Failed to create directory <prefix>/overlay/root")?;

        // Create a lowerdir for /space and /dev
        system::mount("none", format!("{prefix}/ephemeral"), "tmpfs", 0, None)
            .context_invoker("Failed to mount tmpfs on <prefix>/ephemeral")?;
        std::fs::create_dir(format!("{prefix}/ephemeral/space"))
            .context_invoker("Failed to create <prefix>/ephemeral/space")?;
        std::fs::create_dir(format!("{prefix}/ephemeral/dev"))
            .context_invoker("Failed to create <prefix>/ephemeral/dev")?;

        // Mount overlay
        let fs_options = format!(
            "lowerdir={}/{}:{prefix}/ephemeral",
            package
                .image
                .mountpoint
                .to_str()
                .context_invoker("Mountpoint must be a string")?,
            package.name
        );
        system::mount(
            "overlay",
            format!("{prefix}/overlay/root"),
            "overlay",
            0,
            Some(&fs_options),
        )
        .context_invoker("Failed to mount overlay on <prefix>/overlay/root")?;

        // Don't mount /space, because RootFS::reset() will remount it anyway

        // Mount /dev on overlay
        system::bind_mount_opt(
            "/tmp/sunwalker_invoker/dev",
            format!("{prefix}/overlay/root/dev"),
            system::MS_RDONLY,
        )
        .context_invoker("Failed to mount /dev on <prefix>/overlay/root")?;
    } {
        // Rollback
        if let Err(e) = unmount_recursively(&prefix, false) {
            println!(
                "Failed to unmount {prefix} recursively after unsuccessful initialization: {e:?}"
            );
        }
        if let Err(e) = std::fs::remove_dir_all(&prefix) {
            println!("Failed to rm -r {prefix} after unsuccessful initialization: {e:?}");
        }
        return Err(e);
    }

    Ok(RootFS {
        removed: false,
        id,
        bound_files,
        quotas,
    })
}

pub async fn make_namespace(id: String) -> Result<Namespace, errors::Error> {
    let (mut upstream, downstream) = multiprocessing::tokio::duplex::<(), ()>()
        .context_invoker("Failed to create duplex connection to an isolated subprocess")?;

    let mut child = make_ns
        .spawn_tokio(downstream)
        .await
        .context_invoker("Failed to start an isolated subprocess")?;

    let res = upstream
        .recv()
        .await
        .context_invoker("Failed to read start confirmation from the isolated subprocess")?;
    if res.is_none() {
        let res = child
            .join()
            .await
            .context_invoker("Isolated process didn't terminate gracefully")?;
        let err = res
            .err()
            .unwrap_or_else(|| errors::InvokerFailure("(no error reported)".to_string()));
        return Err(err.context_invoker("Isolated process failed to start isolation"));
    }

    // Fill uid/gid maps
    std::fs::write(
        format!("/proc/{}/uid_map", child.id()),
        // Global root stays root, the user is 1000, and nobody is bound just in case
        format!("0 0 1\n1000 1 1\n65534 65534 1\n"),
    )
    .context_invoker("Failed to create uid_map for the isolated subprocess")?;

    std::fs::write(format!("/proc/{}/setgroups", child.id()), "deny\n")
        .context_invoker("Failed to create setgroups for the isolated subprocess")?;

    std::fs::write(
        format!("/proc/{}/gid_map", child.id()),
        "0 0 1\n1000 1 1\n65534 65534 1\n",
    )
    .context_invoker("Failed to create gid_map for the isolated subprocess")?;

    let prefix = &format!("/tmp/sunwalker_invoker/worker/ns/{id}");
    std::fs::create_dir(prefix).context_invoker("Failed to create <prefix>")?;

    if let Err(e) = try {
        // Save namespaces
        for name in ["ipc", "user", "uts", "net"] {
            let orig_path = format!("/proc/{}/ns/{name}", child.id());
            let path = format!("{prefix}/{name}");
            std::fs::write(&path, "")
                .with_context_invoker(|| format!("Failed to create <prefix>/{name}"))?;
            system::bind_mount(&orig_path, &path).with_context_invoker(|| {
                format!("Failed to bind-mount {orig_path} to <prefix>/{name}")
            })?;
        }

        upstream
            .send(&())
            .await
            .context_invoker("Failed to tell the isolated subprocess to terminate")?;

        child
            .join()
            .await
            .context_invoker("Isolated process didn't terminate gracefully")?
    } {
        if let Err(e) = unmount_recursively(prefix, false) {
            println!(
                "Failed to unmount {prefix} recursively after unsuccessful initialization: {e:?}"
            );
        }
        if let Err(e) = std::fs::remove_dir_all(prefix) {
            println!("Failed to rm -r {prefix} after unsuccessful initialization: {e:?}");
        }
        return Err(e);
    }

    Ok(Namespace { removed: false, id })
}

impl RootFS {
    pub fn reset(&self) -> Result<(), errors::Error> {
        let space = format!("{}/space", self.overlay());

        // Unmount /space and everything beneath
        unmount_recursively(&space, true)?;

        // Remount /space
        system::mount(
            "none",
            &space,
            "tmpfs",
            system::MS_NOSUID,
            Some(
                format!(
                    "size={},nr_inodes={}",
                    self.quotas.space, self.quotas.max_inodes
                )
                .as_ref(),
            ),
        )
        .with_context_invoker(|| format!("Mounting tmpfs on {space} failed"))?;

        std::os::unix::fs::chown(&space, Some(1), Some(1))
            .with_context_invoker(|| format!("Failed to chown {space}"))?;

        // Remount /dev/shm
        let space_shm = format!("{space}/.shm");
        let dev_shm = format!("{}/dev/shm", self.overlay());
        std::fs::create_dir(&space_shm)
            .with_context_invoker(|| format!("Failed to create directory at {space_shm}"))?;
        if let Err(e) = system::umount(&dev_shm) {
            if let std::io::ErrorKind::InvalidInput = e.kind() {
                // This means /dev/shm is not a mountpoint, which is fine the first time we run
                // reset()
            } else {
                return Err(e.with_context_invoker(|| format!("Failed to unmount {dev_shm}")));
            }
        }
        system::bind_mount(&space_shm, &dev_shm).with_context_invoker(|| {
            format!(
                "Failed to bind-mount {space_shm} to {}/dev/shm",
                self.overlay()
            )
        })?;

        let overlay = self.overlay();
        for (from, to) in self.bound_files.iter() {
            let to = format!("{overlay}{to}");
            std::fs::write(&to, "").with_context_invoker(|| format!("Failed to create {to}"))?;
            system::bind_mount_opt(from, &to, system::MS_RDONLY)
                .with_context_invoker(|| format!("Failed to bind-mount {from:?} to {to}"))?;
        }

        Ok(())
    }

    fn _remove(&mut self) -> Result<(), errors::Error> {
        if self.removed {
            return Ok(());
        }

        self.removed = true;

        let prefix = format!("/tmp/sunwalker_invoker/worker/rootfs/{}", self.id);
        unmount_recursively(&prefix, false)?;
        std::fs::remove_dir_all(&prefix)
            .with_context_invoker(|| format!("Failed to remove {prefix} recursively"))?;

        Ok(())
    }

    pub fn overlay(&self) -> String {
        format!(
            "/tmp/sunwalker_invoker/worker/rootfs/{}/overlay/root",
            self.id
        )
    }

    pub fn read(&self, path: &str) -> Result<Vec<u8>, errors::Error> {
        let path = format!("{}/{path}", self.overlay());

        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|e| errors::UserFailure(format!("Failed to stat {path:?}: {e:?}")))?;

        if !metadata.is_file() {
            return Err(errors::UserFailure(format!(
                "{path:?} is not a regular file"
            )));
        }

        if metadata.size() > self.quotas.space {
            return Err(errors::UserFailure(format!(
                "Size of {path:?} is more than the maximum size of the filesystem"
            )));
        }

        std::fs::read(&path)
            .map_err(|e| errors::UserFailure(format!("Failed to open {path:?} for reading: {e:?}")))
    }

    pub fn remove(mut self) -> Result<(), errors::Error> {
        self._remove()
    }
}

impl Namespace {
    fn _remove(&mut self) -> Result<(), errors::Error> {
        if self.removed {
            return Ok(());
        }

        self.removed = true;

        let prefix = format!("/tmp/sunwalker_invoker/worker/ns/{}", self.id);
        unmount_recursively(&prefix, false)?;
        std::fs::remove_dir_all(&prefix)
            .with_context_invoker(|| format!("Failed to remove {prefix} recursively"))?;

        Ok(())
    }

    pub fn remove(mut self) -> Result<(), errors::Error> {
        self._remove()
    }
}

impl Drop for RootFS {
    fn drop(&mut self) {
        self._remove()/*.unwrap()*/;
    }
}

impl Drop for Namespace {
    fn drop(&mut self) {
        self._remove()/*.unwrap()*/;
    }
}

pub async fn run_isolated<T: Object + 'static>(
    f: Box<dyn multiprocessing::FnOnce<(), Output = Result<T, errors::Error>> + Send + Sync>,
    rootfs: &RootFS,
    ns: &Namespace,
) -> Result<T, errors::Error> {
    let mut child = isolated_entry
        .spawn_tokio(f, rootfs.id.clone(), ns.id.clone())
        .await
        .context_invoker("Failed to start an isolated subprocess")?;

    child
        .join()
        .await
        .context_invoker("Isolated process didn't terminate gracefully")?
}

#[multiprocessing::entrypoint]
#[tokio::main(flavor = "current_thread")] // unshare requires a single thread
async fn make_ns(mut duplex: multiprocessing::tokio::Duplex<(), ()>) -> Result<(), errors::Error> {
    if unsafe {
        libc::unshare(CLONE_NEWIPC | CLONE_NEWUSER | CLONE_NEWUTS | CLONE_SYSVSEM | CLONE_NEWNET)
    } != 0
    {
        return Err(std::io::Error::last_os_error().context_invoker("Failed to unshare namespaces"));
    }

    // Configure UTS namespace
    let domain_name = "sunwalker";
    if unsafe { libc::setdomainname(domain_name.as_ptr() as *const c_char, domain_name.len()) }
        == -1
    {
        return Err(std::io::Error::last_os_error().context_invoker("Failed to set domain name"));
    }

    let host_name = "invoker";
    if unsafe { libc::sethostname(host_name.as_ptr() as *const c_char, host_name.len()) } == -1 {
        return Err(std::io::Error::last_os_error().context_invoker("Failed to set host name"));
    }

    // Will a reasonable program ever use a local network interface? Theoretically, I can see a
    // runtime with built-in multiprocessing support use a TCP socket on localhost for IPC, but
    // practically, the chances are pretty low and getting the network up takes time, so I'm leaving
    // it disabled for now.
    //
    // The second reason is that enabling it not as easy as flicking a switch. Linux collects
    // statistics on network interfaces, so the the network interfaces have to be re-created every
    // time to prevent data leaks. The lo interface is unique in the way that it always exists in
    // the netns and can't be deleted or recreated, according to a comment in Linux kernel:
    //     The loopback device is special if any other network devices
    //     is present in a network namespace the loopback device must
    //     be present. Since we now dynamically allocate and free the
    //     loopback device ensure this invariant is maintained by
    //     keeping the loopback device as the first device on the
    //     list of network devices.  Ensuring the loopback devices
    //     is the first device that appears and the last network device
    //     that disappears.
    //
    // However, we can create a dummy interface and assign the local addresses to it rather than lo.
    // It would still have to be re-created, though, and that takes precious time, 50 ms for me. And
    // then there is a problem with IPv6--::1 cannot be assigned to anything but lo due to a quirk
    // in the interpretation of the IPv6 RFC by the Linux kernel.

    // Bring lo down
    {
        let (connection, handle, _) =
            rtnetlink::new_connection().context_invoker("Failed to connect to rtnetlink")?;
        tokio::spawn(connection);

        if let Some(link) = handle
            .link()
            .get()
            .match_name("lo".to_string())
            .execute()
            .try_next()
            .await
            .context_invoker("Failed to find lo link")?
        {
            handle
                .link()
                .set(link.header.index)
                .down()
                .execute()
                .await
                .context_invoker("Failed to bring lo down")?;
        }
    }

    // Stop ourselves
    duplex
        .send(&())
        .await
        .context_invoker("Failed to read notify the parent about successful unshare")?;
    duplex
        .recv()
        .await
        .context_invoker("Failed to get stop signal from parent")?
        .context_invoker("Parent died before sending stop signal to the isolated process")?;

    Ok(())
}

#[multiprocessing::entrypoint]
#[tokio::main(flavor = "current_thread")] // unshare requires a single thread
async fn isolated_entry<T: Object + 'static>(
    f: Box<dyn multiprocessing::FnOnce<(), Output = Result<T, errors::Error>> + Send + Sync>,
    rootfs_id: String,
    ns_id: String,
) -> Result<T, errors::Error> {
    // Join prepared namespaces. They are old in the sense that they may contain stray information
    // from previous runs. It's necessary to clean it up to prevent communication between runs.
    for name in ["ipc", "user", "uts", "net"] {
        let path = format!("/tmp/sunwalker_invoker/worker/ns/{ns_id}/{name}");
        let file =
            std::fs::File::open(&path).with_context_invoker(|| format!("Failed to open {path}"))?;
        nix::sched::setns(file.as_raw_fd(), nix::sched::CloneFlags::empty())
            .with_context_invoker(|| format!("Failed to setns {path}"))?;
    }

    // IPC namespace. This is critical to clean up correctly, because creating an IPC namespace in
    // the kernel is terribly slow, and *deleting* it actually happens asynchronously. This
    // basically means that if we create and drop IPC namespaces quickly enough, the deleting queue
    // will overflow and we won't be able to do any IPC operation (including creation of an IPC
    // namespace) for a while--something to avoid at all costs.

    // Clean up System V message queues
    {
        let file = std::fs::File::open("/proc/sysvipc/msg")
            .context_invoker("Failed to open /proc/sysvipc/msg")?;

        let mut msqids: Vec<libc::c_int> = Vec::new();

        // Skip header
        for line in std::io::BufReader::new(file).lines().skip(1) {
            let line = line.context_invoker("Failed to read /proc/sysvipc/msg")?;
            let mut it = line.trim().split_ascii_whitespace();

            it.next()
                .context_invoker("Invalid format of /proc/sysvipc/msg")?;

            let msqid = it
                .next()
                .context_invoker("Invalid format of /proc/sysvipc/msg")?
                .parse()
                .context_invoker("Invalid format of msqid in /proc/sysvipc/msg")?;

            msqids.push(msqid);
        }

        for msqid in msqids {
            if unsafe { libc::msgctl(msqid, libc::IPC_RMID, std::ptr::null_mut()) } == -1 {
                return Err(std::io::Error::last_os_error().with_context_invoker(|| {
                    format!("Failed to delete System V message queue #{msqid}")
                }));
            }
        }
    }

    // Clean up System V semaphores sets
    {
        let file = std::fs::File::open("/proc/sysvipc/sem")
            .context_invoker("Failed to open /proc/sysvipc/sem")?;

        let mut semids: Vec<libc::c_int> = Vec::new();

        // Skip header
        for line in std::io::BufReader::new(file).lines().skip(1) {
            let line = line.context_invoker("Failed to read /proc/sysvipc/sem")?;
            let mut it = line.trim().split_ascii_whitespace();

            it.next()
                .context_invoker("Invalid format of /proc/sysvipc/sem")?;

            let semid = it
                .next()
                .context_invoker("Invalid format of /proc/sysvipc/sem")?
                .parse()
                .context_invoker("Invalid format of semid in /proc/sysvipc/sem")?;

            semids.push(semid);
        }

        for semid in semids {
            if unsafe { libc::semctl(semid, 0, libc::IPC_RMID) } == -1 {
                return Err(std::io::Error::last_os_error().with_context_invoker(|| {
                    format!("Failed to delete System V semaphore #{semid}")
                }));
            }
        }
    }

    // Clean up System V shared memory segments
    {
        let file = std::fs::File::open("/proc/sysvipc/shm")
            .context_invoker("Failed to open /proc/sysvipc/shm")?;

        let mut shmids: Vec<libc::c_int> = Vec::new();

        // Skip header
        for line in std::io::BufReader::new(file).lines().skip(1) {
            let line = line.context_invoker("Failed to read /proc/sysvipc/shm")?;
            let mut it = line.trim().split_ascii_whitespace();

            it.next()
                .context_invoker("Invalid format of /proc/sysvipc/shm")?;

            let shmid = it
                .next()
                .context_invoker("Invalid format of /proc/sysvipc/shm")?
                .parse()
                .context_invoker("Invalid format of shmid in /proc/sysvipc/shm")?;

            shmids.push(shmid);
        }

        for shmid in shmids {
            if unsafe { libc::shmctl(shmid, libc::IPC_RMID, std::ptr::null_mut()) } == -1 {
                return Err(std::io::Error::last_os_error().with_context_invoker(|| {
                    format!("Failed to delete System V shared memory #{shmid}")
                }));
            }
        }
    }

    // POSIX message queues are handled below

    // Unshare mount, PID, and network namespaces:
    // - We remount overlay in the parent, and new mounts in the parent namespace don't propagate to
    //   the child namespace, so we have to create a new mountns every time;
    // - A PID namespace it's not usable after init (the process with pid 1, that is) dies, and we
    //   can't make sure our init wasn't tampered with if we reuse the namespace;
    if unsafe { libc::unshare(CLONE_NEWNS | CLONE_NEWPID) } != 0 {
        return Err(
            std::io::Error::last_os_error().context_invoker("Failed to unshare mount namespace")
        );
    }

    // Switch to root user
    if unsafe { libc::setuid(0) } != 0 {
        return Err(std::io::Error::last_os_error()
            .context_invoker("setuid(0) failed while entering sandbox"));
    }
    if unsafe { libc::setgid(0) } != 0 {
        return Err(std::io::Error::last_os_error()
            .context_invoker("setgid(0) failed while entering sandbox"));
    }

    // Instead of pivot_root'ing directly into .../overlay/root, we pivot_root into .../overlay
    // first and chroot into /root second. There are two reasons for this inefficiency:
    //
    // 1. We prefer pivot_root to chroot because that allows us to unmount the old root, which
    // a) prevents various chroot exploits from working, because there's no old root to return to
    // anyway, and b) enables slightly more efficient mount namespace management and avoids
    // unnecessary locking.
    //
    // 2. The resulting environment must be chrooted, because that prevents unshare(CLONE_NEWUSER)
    // from succeeding inside the namespace. This is, in fact, the only way to do this without
    // spooky action at a distance, that I am aware of. This used to be an implementation detail of
    // the Linux kernel, but should perhaps be considered more stable now. The necessity to disable
    // user namespaces comes not from their intrinsic goal but from the fact that they enable all
    // other namespaces to work without root, and while most of them are harmless (e.g. network and
    // PID namespaces), others may be used to bypass quotas (not other security measures, though).
    // One prominent example is mount namespace, which enables the user to mount a read-write tmpfs
    // without disk limits and use it as unlimited temporary storage to exceed the memory limit.

    // pivot_root requires the new root to be a mount, and for it not to be MNT_LOCKED (the reason
    // for which I don't quite understand). The simplest way to do that is to bind-mount .../overlay
    // onto itself. Note that if we pivot_root'ed into .../overlay/root, we'd need to bind-mount
    // itself anyway because the kernel marks .../overlay/root as MNT_LOCKED as a safety restriction
    // due to the use of user namespaces.
    let overlay = format!("/tmp/sunwalker_invoker/worker/rootfs/{rootfs_id}/overlay");

    system::bind_mount_opt(&overlay, &overlay, system::MS_REC)
        .with_context_invoker(|| format!("Failed to bind-mount {overlay} onto itself"))?;

    // Change root to .../overlay
    std::env::set_current_dir(&overlay)
        .with_context_invoker(|| format!("Failed to chdir to new root at {overlay}"))?;
    nix::unistd::pivot_root(".", ".").context_invoker("Failed to pivot_root")?;
    system::umount_opt(".", system::MNT_DETACH).context_invoker("Failed to unmount self")?;

    // Chroot into .../overlay/root
    std::env::set_current_dir("/root").context_invoker("Failed to chdir to /root")?;
    nix::unistd::chroot(".").context_invoker("Failed to chroot into /root")?;

    // POSIX message queues are stored in /dev/mqueue, which we can simply remount instead of
    // cleaning up queue-by-queue. It is also sort of a necessity, because the IPC that the mqueuefs
    // is related to depends on when it's mounted.
    system::mount("mqueue", "/dev/mqueue", "mqueue", 0, None)
        .context_invoker("Failed to mount /dev/mqueue")?;
    // rwxrwxrwt
    std::fs::set_permissions("/dev/mqueue", std::fs::Permissions::from_mode(0o1777))
        .context_invoker("Failed to make /dev/mqueue world-writable")?;

    // Expose defaults for environment variables
    std::env::set_var(
        "LD_LIBRARY_PATH",
        "/usr/local/lib64:/usr/local/lib:/usr/lib64:/usr/lib:/lib64:/lib",
    );
    std::env::set_var("LANGUAGE", "en_US");
    std::env::set_var("LC_ALL", "en_US.UTF-8");
    std::env::set_var("LC_ADDRESS", "en_US.UTF-8");
    std::env::set_var("LC_NAME", "en_US.UTF-8");
    std::env::set_var("LC_MONETARY", "en_US.UTF-8");
    std::env::set_var("LC_PAPER", "en_US.UTF-8");
    std::env::set_var("LC_IDENTIFIER", "en_US.UTF-8");
    std::env::set_var("LC_TELEPHONE", "en_US.UTF-8");
    std::env::set_var("LC_MEASUREMENT", "en_US.UTF-8");
    std::env::set_var("LC_TIME", "en_US.UTF-8");
    std::env::set_var("LC_NUMERIC", "en_US.UTF-8");
    std::env::set_var("LANG", "en_US.UTF-8");

    // Use environment from the package
    let file = std::fs::File::open("/.sunwalker/env").map_err(|e| {
        errors::ConfigurationFailure(format!("Failed to open /.sunwalker/env for reading: {e:?}"))
    })?;
    for line in std::io::BufReader::new(file).lines() {
        let line = line.map_err(|e| {
            errors::ConfigurationFailure(format!("Failed to read from /.sunwalker/env: {e:?}"))
        })?;
        let idx = line.find('=').ok_or_else(|| {
            errors::ConfigurationFailure(format!(
                "'=' not found in a line of /.sunwalker/env: {line}"
            ))
        })?;
        let (name, value) = line.split_at(idx);
        let value = &value[1..];
        std::env::set_var(name, value);
    }

    // Switch to fake user
    if unsafe { libc::setgid(1000) } != 0 {
        return Err(std::io::Error::last_os_error()
            .context_invoker("setgid(1000) failed while entering sandbox"));
    }
    if unsafe { libc::setuid(1000) } != 0 {
        return Err(std::io::Error::last_os_error()
            .context_invoker("setuid(1000) failed while entering sandbox"));
    }

    f()
}
