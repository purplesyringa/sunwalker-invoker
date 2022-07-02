use crate::{cgroups, errors, image::package, system};
use futures_util::TryStreamExt;
use libc::{
    c_char, CLONE_NEWIPC, CLONE_NEWNET, CLONE_NEWNS, CLONE_NEWPID, CLONE_NEWUSER, CLONE_NEWUTS,
    CLONE_SYSVSEM,
};
use multiprocessing::Object;
use std::ffi::{CString, OsString};
use std::io::BufRead;
use std::os::unix::{
    fs::MetadataExt,
    {ffi::OsStringExt, io::AsRawFd},
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
fn unmount_recursively(prefix: &str) -> Result<(), errors::Error> {
    let prefix_slash = format!("{}/", prefix);

    let file = std::fs::File::open("/proc/self/mounts").map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to open /proc/self/mounts for reading: {:?}",
            e
        ))
    })?;

    let mut vec = Vec::new();
    for line in std::io::BufReader::new(file).lines() {
        let line = line.map_err(|e| {
            errors::InvokerFailure(format!("Failed to read /proc/self/mounts: {:?}", e))
        })?;
        let mut it = line.split(" ");
        it.next().ok_or_else(|| {
            errors::InvokerFailure("Invalid format of /proc/self/mounts".to_string())
        })?;
        let target_path = it.next().ok_or_else(|| {
            errors::InvokerFailure("Invalid format of /proc/self/mounts".to_string())
        })?;
        if target_path.starts_with(&prefix_slash) {
            vec.push(target_path.to_string());
        }
    }

    for path in vec.into_iter().rev() {
        system::umount(&path)
            .map_err(|e| errors::InvokerFailure(format!("Failed to unmount {}: {:?}", path, e)))?;
    }

    Ok(())
}

pub fn enter_worker_space(core: u64) -> Result<(), errors::Error> {
    // Unshare namespaces
    unsafe {
        if libc::unshare(CLONE_NEWNS) != 0 {
            return Err(errors::InvokerFailure(format!(
                "Failed to unshare mount namespace: {:?}",
                std::io::Error::last_os_error()
            )));
        }
    }

    // Create per-worker tmpfs
    system::mount("none", "/tmp/sunwalker_invoker/worker", "tmpfs", 0, None).map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to mount tmpfs on /tmp/sunwalker_invoker/worker: {:?}",
            e
        ))
    })?;

    std::fs::create_dir("/tmp/sunwalker_invoker/worker/rootfs").map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to create /tmp/sunwalker_invoker/worker/rootfs: {:?}",
            e
        ))
    })?;
    std::fs::create_dir("/tmp/sunwalker_invoker/worker/ns").map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to create /tmp/sunwalker_invoker/worker/ns: {:?}",
            e
        ))
    })?;
    std::fs::create_dir("/tmp/sunwalker_invoker/worker/aux").map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to create /tmp/sunwalker_invoker/worker/aux: {:?}",
            e
        ))
    })?;

    // Switch to core
    let pid = unsafe { libc::getpid() };
    cgroups::add_task_to_core(pid, core).map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to move current process (PID {}) to core {}: {:?}",
            pid, core, e
        ))
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

    let prefix = format!("/tmp/sunwalker_invoker/worker/rootfs/{}", id);

    std::fs::create_dir(&prefix).map_err(|e| {
        errors::InvokerFailure(format!("Failed to mount tmpfs on {}: {:?}", prefix, e))
    })?;
    std::fs::create_dir(format!("{}/ephemeral", prefix)).map_err(|e| {
        errors::InvokerFailure(format!("Failed to create {}/ephemeral: {:?}", prefix, e))
    })?;
    std::fs::create_dir(format!("{}/overlay", prefix)).map_err(|e| {
        errors::InvokerFailure(format!("Failed to create {}/overlay: {:?}", prefix, e))
    })?;
    std::fs::create_dir(format!("{}/overlay/root", prefix)).map_err(|e| {
        errors::InvokerFailure(format!("Failed to create {}/overlay/root: {:?}", prefix, e))
    })?;

    // Create a lowerdir for /space and /dev
    system::mount("none", format!("{}/ephemeral", prefix), "tmpfs", 0, None).map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to mount tmpfs on {}/ephemeral: {:?}",
            prefix, e
        ))
    })?;
    std::fs::create_dir(format!("{}/ephemeral/space", prefix)).map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to create {}/ephemeral/space: {:?}",
            prefix, e
        ))
    })?;
    std::fs::create_dir(format!("{}/ephemeral/dev", prefix)).map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to create {}/ephemeral/dev: {:?}",
            prefix, e
        ))
    })?;

    // Mount overlay
    let fs_options = format!(
        "lowerdir={}/{}:{}/ephemeral",
        package
            .image
            .mountpoint
            .to_str()
            .ok_or_else(|| errors::InvokerFailure("Mountpoint must be a string".to_string()))?,
        package.name,
        prefix,
    );
    system::mount(
        "overlay",
        format!("{}/overlay/root", prefix),
        "overlay",
        0,
        Some(&fs_options),
    )
    .map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to mount overlay at {}/overlay/root: {:?}",
            prefix, e
        ))
    })?;

    // Make /space a tmpfs with the necessary disk quotas
    system::mount(
        "none",
        format!("{}/overlay/root/space", prefix),
        "tmpfs",
        system::MS_NOSUID,
        Some(format!("size={},nr_inodes={}", quotas.space, quotas.max_inodes).as_ref()),
    )
    .map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to mount tmpfs on {}/overlay/root/space: {:?}",
            prefix, e
        ))
    })?;

    // Mount /dev on overlay
    system::bind_mount_opt(
        "/tmp/sunwalker_invoker/dev",
        format!("{}/overlay/root/dev", prefix),
        system::MS_RDONLY,
    )
    .map_err(|e| {
        errors::InvokerFailure(format!("Failed to mount /dev on .../overlay/root: {:?}", e))
    })?;

    // Mount /dev/shm on overlay
    std::fs::create_dir(format!("{}/overlay/root/space/.shm", prefix)).map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to create directory at {}/overlay/root/space/.shm: {:?}",
            prefix, e
        ))
    })?;
    system::bind_mount(
        format!("{}/overlay/root/space/.shm", prefix),
        format!("{}/overlay/root/dev/shm", prefix),
    )
    .map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to mount /dev/shm on .../overlay/root: {:?}",
            e
        ))
    })?;

    // Allow the sandbox user to write to /space
    std::os::unix::fs::chown(
        format!("{}/overlay/root/space", prefix),
        Some(65534),
        Some(65534),
    )
    .map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to chown {}/overlay/root/space: {:?}",
            prefix, e
        ))
    })?;

    // Initialize user directory.
    for (from, to) in bound_files.iter() {
        let to = format!("{}/overlay/root{}", prefix, to);

        std::fs::write(&to, "")
            .map_err(|e| errors::InvokerFailure(format!("Failed to create {}: {:?}", to, e)))?;
        system::bind_mount_opt(from, &to, system::MS_RDONLY).map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to bind-mount {:?} to {}: {:?}",
                from, to, e
            ))
        })?;
    }

    Ok(RootFS {
        removed: false,
        id,
        bound_files,
        quotas,
    })
}

pub async fn make_namespace(id: String) -> Result<Namespace, errors::Error> {
    let (mut upstream, downstream) = multiprocessing::tokio::duplex::<(), ()>().map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to create duplex connection to an isolated subprocess: {:?}",
            e
        ))
    })?;

    let mut child = make_ns.spawn_tokio(downstream).await.map_err(|e| {
        errors::InvokerFailure(format!("Failed to start an isolated subprocess: {:?}", e))
    })?;

    let res = upstream.recv().await.map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to read start confirmation from the isolated subprocess: {:?}",
            e
        ))
    })?;
    if res.is_none() {
        let res = child.join().await.map_err(|e| {
            errors::InvokerFailure(format!(
                "Isolated process didn't terminate gracefully: {:?}",
                e
            ))
        })?;
        let err = res
            .err()
            .unwrap_or_else(|| errors::InvokerFailure("(no error reported)".to_string()));
        return Err(errors::InvokerFailure(format!(
            "Isolated process failed to start isolation: {:?}",
            err
        )));
    }

    // Fill uid/gid maps and switch to
    std::fs::write(
        format!("/proc/{}/uid_map", child.id()),
        format!("0 65534 1\n"),
    )
    .map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to create uid_map for the isolated subprocess: {:?}",
            e
        ))
    })?;

    std::fs::write(format!("/proc/{}/setgroups", child.id()), "deny\n").map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to create setgroups for the isolated subprocess: {:?}",
            e
        ))
    })?;

    std::fs::write(
        format!("/proc/{}/gid_map", child.id()),
        format!("0 65534 1\n"),
    )
    .map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to create gid_map for the isolated subprocess: {:?}",
            e
        ))
    })?;

    let prefix = &format!("/tmp/sunwalker_invoker/worker/ns/{}", id);
    std::fs::create_dir(prefix)
        .map_err(|e| errors::InvokerFailure(format!("Failed to create {}: {:?}", prefix, e)))?;

    (async move || {
        for name in ["ipc", "user", "uts", "net"] {
            let path = format!("{}/{}", prefix, name);
            std::fs::write(&path, "").map_err(|e| {
                errors::InvokerFailure(format!("Failed to create {}: {:?}", path, e))
            })?;
            system::bind_mount(&format!("/proc/{}/ns/{}", child.id(), name), &path).map_err(
                |e| {
                    errors::InvokerFailure(format!(
                        "Failed to bind-mount /proc/{}/ns/{} to {}: {:?}",
                        child.id(),
                        name,
                        path,
                        e
                    ))
                },
            )?;
        }

        upstream.send(&()).await.map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to tell the isolated subprocess to terminate: {:?}",
                e
            ))
        })?;

        child.join().await.map_err(|e| {
            errors::InvokerFailure(format!(
                "Isolated process didn't terminate gracefully: {:?}",
                e
            ))
        })?
    })()
    .await
    .map_err(|e| {
        if let Err(e) = unmount_recursively(&prefix) {
            println!(
                "Failed to unmount {} recursively after unsuccessful initialization: {:?}",
                prefix, e
            );
        }
        if let Err(e) = std::fs::remove_dir_all(&prefix) {
            println!(
                "Failed to rm -r {} after unsuccessful initialization: {:?}",
                prefix, e
            );
        }
        e
    })?;

    Ok(Namespace { removed: false, id })
}

impl RootFS {
    pub fn reset(&self) -> Result<(), errors::Error> {
        let space = format!("{}/space", self.overlay());

        // Unmount everything under /space
        unmount_recursively(&space)?;

        // Remount /space
        system::umount(&space)
            .map_err(|e| errors::InvokerFailure(format!("Failed to unmount {}: {:?}", space, e)))?;

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
        .map_err(|e| {
            errors::InvokerFailure(format!("Mounting tmpfs on {} failed: {:?}", space, e))
        })?;

        std::os::unix::fs::chown(&space, Some(65534), Some(65534))
            .map_err(|e| errors::InvokerFailure(format!("Failed to chown {}: {:?}", space, e)))?;

        // Remount /dev/shm
        let space_shm = format!("{}/.shm", space);
        let dev_shm = format!("{}/dev/shm", self.overlay());
        std::fs::create_dir(&space_shm).map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to create directory at {}: {:?}",
                space_shm, e
            ))
        })?;
        system::umount(&dev_shm).map_err(|e| {
            errors::InvokerFailure(format!("Failed to unmount {}: {:?}", dev_shm, e))
        })?;
        system::bind_mount(&space_shm, &dev_shm).map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to bind-mount {} to {}/dev/shm: {:?}",
                space_shm,
                self.overlay(),
                e
            ))
        })?;

        let overlay = self.overlay();
        for (from, to) in self.bound_files.iter() {
            let to = format!("{}{}", overlay, to);
            std::fs::write(&to, "")
                .map_err(|e| errors::InvokerFailure(format!("Failed to create {}: {:?}", to, e)))?;
            system::bind_mount_opt(from, &to, system::MS_RDONLY).map_err(|e| {
                errors::InvokerFailure(format!(
                    "Failed to bind-mount {:?} to {}: {:?}",
                    from, to, e
                ))
            })?;
        }

        Ok(())
    }

    fn _remove(&mut self) -> Result<(), errors::Error> {
        if self.removed {
            return Ok(());
        }

        self.removed = true;

        let prefix = format!("/tmp/sunwalker_invoker/worker/rootfs/{}", self.id);
        unmount_recursively(&prefix)?;
        std::fs::remove_dir_all(&prefix).map_err(|e| {
            errors::InvokerFailure(format!("Failed to remove {} recursively: {:?}", prefix, e))
        })?;

        Ok(())
    }

    pub fn overlay(&self) -> String {
        format!(
            "/tmp/sunwalker_invoker/worker/rootfs/{}/overlay/root",
            self.id
        )
    }

    pub fn read(&self, path: &str) -> Result<Vec<u8>, errors::Error> {
        let path = format!("{}/{}", self.overlay(), path);

        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|e| errors::UserFailure(format!("Failed to stat {:?}: {:?}", path, e)))?;

        if !metadata.is_file() {
            return Err(errors::UserFailure(format!(
                "{:?} is not a regular file",
                path
            )));
        }

        if metadata.size() > self.quotas.space {
            return Err(errors::UserFailure(format!(
                "Size of {:?} is more than the maximum size of the filesystem",
                path
            )));
        }

        std::fs::read(&path).map_err(|e| {
            errors::UserFailure(format!("Failed to open {:?} for reading: {:?}", path, e))
        })
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
        unmount_recursively(&prefix)?;
        std::fs::remove_dir_all(&prefix).map_err(|e| {
            errors::InvokerFailure(format!("Failed to remove {} recursively: {:?}", prefix, e))
        })?;

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
        .map_err(|e| {
            errors::InvokerFailure(format!("Failed to start an isolated subprocess: {:?}", e))
        })?;

    child.join().await.map_err(|e| {
        errors::InvokerFailure(format!(
            "Isolated process didn't terminate gracefully: {:?}",
            e
        ))
    })?
}

#[multiprocessing::entrypoint]
#[tokio::main(flavor = "current_thread")] // unshare requires a single thread
async fn make_ns(mut duplex: multiprocessing::tokio::Duplex<(), ()>) -> Result<(), errors::Error> {
    if unsafe {
        libc::unshare(CLONE_NEWIPC | CLONE_NEWUSER | CLONE_NEWUTS | CLONE_SYSVSEM | CLONE_NEWNET)
    } != 0
    {
        return Err(errors::InvokerFailure(format!(
            "Failed to unshare namespaces: {:?}",
            std::io::Error::last_os_error()
        )));
    }

    // Configure UTS namespace
    let domain_name = "sunwalker";
    if unsafe { libc::setdomainname(domain_name.as_ptr() as *const c_char, domain_name.len()) }
        == -1
    {
        return Err(errors::InvokerFailure(format!(
            "Failed to set domain name: {:?}",
            std::io::Error::last_os_error()
        )));
    }

    let host_name = "invoker";
    if unsafe { libc::sethostname(host_name.as_ptr() as *const c_char, host_name.len()) } == -1 {
        return Err(errors::InvokerFailure(format!(
            "Failed to set host name: {:?}",
            std::io::Error::last_os_error()
        )));
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
        let (connection, handle, _) = rtnetlink::new_connection().map_err(|e| {
            errors::InvokerFailure(format!("Failed to connect to rtnetlink: {:?}", e))
        })?;
        tokio::spawn(connection);

        if let Some(link) = handle
            .link()
            .get()
            .match_name("lo".to_string())
            .execute()
            .try_next()
            .await
            .map_err(|e| errors::InvokerFailure(format!("Failed to find lo link: {:?}", e)))?
        {
            handle
                .link()
                .set(link.header.index)
                .down()
                .execute()
                .await
                .map_err(|e| errors::InvokerFailure(format!("Failed to bring lo down: {:?}", e)))?;
        }
    }

    // Stop ourselves
    duplex.send(&()).await.map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to read notify the parent about successful unshare: {:?}",
            e
        ))
    })?;
    duplex
        .recv()
        .await
        .map_err(|e| {
            errors::InvokerFailure(format!("Failed to get stop signal from parent: {:?}", e))
        })?
        .ok_or_else(|| {
            errors::InvokerFailure(
                "Parent died before sending stop signal to the isolated process".to_string(),
            )
        })?;

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
        let path = format!("/tmp/sunwalker_invoker/worker/ns/{}/{}", ns_id, name);
        let file = std::fs::File::open(&path)
            .map_err(|e| errors::InvokerFailure(format!("Failed to open {}: {:?}", path, e)))?;
        nix::sched::setns(file.as_raw_fd(), nix::sched::CloneFlags::empty())
            .map_err(|e| errors::InvokerFailure(format!("Failed to setns {}: {:?}", path, e)))?;
    }

    // IPC namespace. This is critical to clean up correctly, because creating an IPC namespace in
    // the kernel is terribly slow, and *deleting* it actually happens asynchronously. This
    // basically means that if we create and drop IPC namespaces quickly enough, the deleting queue
    // will overflow and we won't be able to do any IPC operation (including creation of an IPC
    // namespace) for a while--something to avoid at all costs.

    // Clean up System V message queues
    {
        let file = std::fs::File::open("/proc/sysvipc/msg").map_err(|e| {
            errors::InvokerFailure(format!("Failed to open /proc/sysvipc/msg: {:?}", e))
        })?;

        let mut msqids: Vec<libc::c_int> = Vec::new();

        // Skip header
        for line in std::io::BufReader::new(file).lines().skip(1) {
            let line = line.map_err(|e| {
                errors::InvokerFailure(format!("Failed to read /proc/sysvipc/msg: {:?}", e))
            })?;
            let mut it = line.trim().split_ascii_whitespace();

            it.next().ok_or_else(|| {
                errors::InvokerFailure("Invalid format of /proc/sysvipc/msg".to_string())
            })?;

            let msqid = it
                .next()
                .ok_or_else(|| {
                    errors::InvokerFailure("Invalid format of /proc/sysvipc/msg".to_string())
                })?
                .parse()
                .map_err(|e| {
                    errors::InvokerFailure(format!(
                        "Invalid format of msqid in /proc/sysvipc/msg: {:?}",
                        e
                    ))
                })?;

            msqids.push(msqid);
        }

        for msqid in msqids {
            if unsafe { libc::msgctl(msqid, libc::IPC_RMID, std::ptr::null_mut()) } == -1 {
                return Err(errors::InvokerFailure(format!(
                    "Failed to delete System V message queue #{}: {:?}",
                    msqid,
                    std::io::Error::last_os_error()
                )));
            }
        }
    }

    // Clean up System V semaphores sets
    {
        let file = std::fs::File::open("/proc/sysvipc/sem").map_err(|e| {
            errors::InvokerFailure(format!("Failed to open /proc/sysvipc/sem: {:?}", e))
        })?;

        let mut semids: Vec<libc::c_int> = Vec::new();

        // Skip header
        for line in std::io::BufReader::new(file).lines().skip(1) {
            let line = line.map_err(|e| {
                errors::InvokerFailure(format!("Failed to read /proc/sysvipc/sem: {:?}", e))
            })?;
            let mut it = line.trim().split_ascii_whitespace();

            it.next().ok_or_else(|| {
                errors::InvokerFailure("Invalid format of /proc/sysvipc/sem".to_string())
            })?;

            let semid = it
                .next()
                .ok_or_else(|| {
                    errors::InvokerFailure("Invalid format of /proc/sysvipc/sem".to_string())
                })?
                .parse()
                .map_err(|e| {
                    errors::InvokerFailure(format!(
                        "Invalid format of semid in /proc/sysvipc/sem: {:?}",
                        e
                    ))
                })?;

            semids.push(semid);
        }

        for semid in semids {
            if unsafe { libc::semctl(semid, 0, libc::IPC_RMID) } == -1 {
                return Err(errors::InvokerFailure(format!(
                    "Failed to delete System V semaphore #{}: {:?}",
                    semid,
                    std::io::Error::last_os_error()
                )));
            }
        }
    }

    // Clean up System V shared memory segments
    {
        let file = std::fs::File::open("/proc/sysvipc/shm").map_err(|e| {
            errors::InvokerFailure(format!("Failed to open /proc/sysvipc/shm: {:?}", e))
        })?;

        let mut shmids: Vec<libc::c_int> = Vec::new();

        // Skip header
        for line in std::io::BufReader::new(file).lines().skip(1) {
            let line = line.map_err(|e| {
                errors::InvokerFailure(format!("Failed to read /proc/sysvipc/shm: {:?}", e))
            })?;
            let mut it = line.trim().split_ascii_whitespace();

            it.next().ok_or_else(|| {
                errors::InvokerFailure("Invalid format of /proc/sysvipc/shm".to_string())
            })?;

            let shmid = it
                .next()
                .ok_or_else(|| {
                    errors::InvokerFailure("Invalid format of /proc/sysvipc/shm".to_string())
                })?
                .parse()
                .map_err(|e| {
                    errors::InvokerFailure(format!(
                        "Invalid format of shmid in /proc/sysvipc/shm: {:?}",
                        e
                    ))
                })?;

            shmids.push(shmid);
        }

        for shmid in shmids {
            if unsafe { libc::shmctl(shmid, libc::IPC_RMID, std::ptr::null_mut()) } == -1 {
                return Err(errors::InvokerFailure(format!(
                    "Failed to delete System V shared memory #{}: {:?}",
                    shmid,
                    std::io::Error::last_os_error()
                )));
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
        return Err(errors::InvokerFailure(format!(
            "Failed to unshare mount namespace: {:?}",
            std::io::Error::last_os_error()
        )));
    }

    // Switch to fake root user
    if unsafe { libc::setuid(0) } != 0 {
        return Err(errors::InvokerFailure(format!(
            "setuid(0) failed while entering sandbox: {:?}",
            std::io::Error::last_os_error()
        )));
    }
    if unsafe { libc::setgid(0) } != 0 {
        return Err(errors::InvokerFailure(format!(
            "setgid(0) failed while entering sandbox: {:?}",
            std::io::Error::last_os_error()
        )));
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
    let overlay = format!("/tmp/sunwalker_invoker/worker/rootfs/{}/overlay", rootfs_id);

    system::bind_mount_opt(&overlay, &overlay, system::MS_REC).map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to bind-mount {} onto itself: {:?}",
            overlay, e
        ))
    })?;

    // Change root to .../overlay
    std::env::set_current_dir(&overlay).map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to chdir to new root at {}: {:?}",
            overlay, e
        ))
    })?;
    nix::unistd::pivot_root(".", ".")
        .map_err(|e| errors::InvokerFailure(format!("Failed to pivot_root: {:?}", e)))?;
    system::umount_opt(".", system::MNT_DETACH)
        .map_err(|e| errors::InvokerFailure(format!("Failed to unmount self: {:?}", e)))?;

    // Chroot into .../overlay/root
    std::env::set_current_dir("/root")
        .map_err(|e| errors::InvokerFailure(format!("Failed to chdir to /root: {:?}", e)))?;
    nix::unistd::chroot(".")
        .map_err(|e| errors::InvokerFailure(format!("Failed to chroot into /root: {:?}", e)))?;

    // POSIX message queues are stored in /dev/mqueue, which we can simply remount. It is also sort
    // of a necessity, because the IPC that the mqueuefs is related to depends on when it's mounted.
    system::mount("mqueue", "/dev/mqueue", "mqueue", 0, None)
        .map_err(|e| errors::InvokerFailure(format!("Failed to mount /dev/mqueue: {:?}", e)))?;

    // Clean up POSIX message queues now, because we need to read /dev/mqueue from inside the
    // filesystem sandbox
    for file in std::fs::read_dir("/dev/mqueue")
        .map_err(|e| errors::InvokerFailure(format!("Failed to enumerate /dev/mqueue: {:?}", e)))?
    {
        let file = file.map_err(|e| {
            errors::InvokerFailure(format!("Failed to enumerate /dev/mqueue: {:?}", e))
        })?;
        let mut mq_path = OsString::from("/");
        mq_path.push(&file.file_name());
        let mq_path = CString::new(mq_path.into_vec()).unwrap();
        nix::mqueue::mq_unlink(&mq_path).map_err(|e| {
            errors::InvokerFailure(format!("Failed to unlink queue {:?}: {:?}", mq_path, e))
        })?;
    }

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
        errors::ConfigurationFailure(format!(
            "Failed to open /.sunwalker/env for reading: {:?}",
            e
        ))
    })?;
    for line in std::io::BufReader::new(file).lines() {
        let line = line.map_err(|e| {
            errors::ConfigurationFailure(format!("Failed to read from /.sunwalker/env: {:?}", e))
        })?;
        let idx = line.find('=').ok_or_else(|| {
            errors::ConfigurationFailure(format!(
                "'=' not found in a line of /.sunwalker/env: {}",
                line
            ))
        })?;
        let (name, value) = line.split_at(idx);
        let value = &value[1..];
        std::env::set_var(name, value);
    }

    f()
}
