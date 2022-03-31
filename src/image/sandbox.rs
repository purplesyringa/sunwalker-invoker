use crate::{cgroups, image::package, process, system};
use anyhow::{anyhow, bail, Context, Result};
use libc::{
    c_int, CLONE_NEWIPC, CLONE_NEWNET, CLONE_NEWNS, CLONE_NEWPID, CLONE_NEWUSER, CLONE_NEWUTS,
    CLONE_SYSVSEM, SIGCONT, SIGSTOP, WUNTRACED,
};
use std::io::BufRead;
use std::panic::UnwindSafe;
use std::path::PathBuf;

#[derive(Clone)]
pub struct SandboxConfig {
    pub max_size_in_bytes: u64,
    pub max_inodes: u64,
    pub core: u64,
}

pub struct WorkerSpace {
    sandbox_config: SandboxConfig,
}

pub struct RootFS<'a> {
    worker_space: &'a WorkerSpace,
    removed: bool,
}

pub fn enter_worker_space(sandbox_config: SandboxConfig) -> Result<WorkerSpace> {
    // Unshare namespaces
    unsafe {
        if libc::unshare(CLONE_NEWNS) != 0 {
            bail!("Could not unshare mount namespace");
        }
    }

    // Create per-worker tmpfs
    system::mount(
        "none",
        "/tmp/worker",
        "tmpfs",
        0,
        Some(
            format!(
                "size={},nr_inodes={}",
                sandbox_config.max_size_in_bytes, sandbox_config.max_inodes
            )
            .as_ref(),
        ),
    )
    .with_context(|| "Mounting tmpfs on /tmp/worker failed")?;

    // Switch to core
    let pid = unsafe { libc::getpid() };
    cgroups::add_task_to_core(pid, sandbox_config.core).with_context(|| {
        format!(
            "Failed to move current process (PID {}) to core {}",
            pid, sandbox_config.core
        )
    })?;

    Ok(WorkerSpace { sandbox_config })
}

impl WorkerSpace {
    pub fn make_rootfs(
        &self,
        package: &package::Package,
        bound_files: Vec<(PathBuf, String)>,
    ) -> Result<RootFS> {
        std::fs::create_dir("/tmp/worker/user-area")?;
        std::fs::create_dir("/tmp/worker/work")?;
        std::fs::create_dir("/tmp/worker/overlay")?;

        // Mount overlay
        system::mount(
            "overlay",
            "/tmp/worker/overlay",
            "overlay",
            0,
            Some(&format!(
                "lowerdir={}/{},upperdir=/tmp/worker/user-area,workdir=/tmp/worker/work",
                package
                    .image
                    .mountpoint
                    .to_str()
                    .expect("Mountpoint must be a string"),
                package.name
            )),
        )
        .with_context(|| "Failed to mount overlay")?;

        // Initialize user directory
        std::fs::create_dir("/tmp/worker/overlay/space")
            .with_context(|| "Failed to create .../space")?;
        for (from, to) in bound_files.into_iter() {
            let to = format!("/tmp/worker/overlay{}", to);
            std::fs::write(&to, "")
                .with_context(|| format!("Failed to create file {:?} on overlay", to))?;
            system::bind_mount_opt(&from, &to, system::MS_RDONLY).with_context(|| {
                format!("Failed to bind-mount {:?} -> {:?} on overlay", from, to)
            })?;
        }

        // Mount /dev on overlay
        std::fs::create_dir("/tmp/worker/overlay/dev")
            .with_context(|| "Failed to create .../dev")?;
        system::bind_mount_opt("/tmp/dev", "/tmp/worker/overlay/dev", system::MS_RDONLY)
            .with_context(|| "Failed to mount /dev on overlay")?;

        // Allow the sandbox user to access data
        std::os::unix::fs::chown("/tmp/worker/overlay/space", Some(65534), Some(65534))?;

        Ok(RootFS {
            worker_space: self,
            removed: false,
        })
    }
}

impl RootFS<'_> {
    pub async fn run_isolated<F: FnOnce() -> () + Send + UnwindSafe>(&self, f: F) -> Result<()> {
        let child = process::scoped_async(|| {
            // Unshare namespaces
            if unsafe {
                libc::unshare(
                    CLONE_NEWNS
                        | CLONE_NEWIPC
                        | CLONE_NEWNET
                        | CLONE_NEWUSER
                        | CLONE_NEWUTS
                        | CLONE_SYSVSEM
                        | CLONE_NEWPID,
                )
            } != 0
            {
                panic!("Could not unshare mount namespace");
            }

            // Stop ourselves
            if unsafe { libc::raise(SIGSTOP) } != 0 {
                panic!("raise(SIGSTOP) failed");
            }

            // Switch to fake root user
            if unsafe { libc::setuid(0) } != 0 {
                let e: Result<(), std::io::Error> = Err(std::io::Error::last_os_error());
                e.with_context(|| "setuid(0) failed while entering sandbox")
                    .unwrap();
            }
            if unsafe { libc::setgid(0) } != 0 {
                let e: Result<(), std::io::Error> = Err(std::io::Error::last_os_error());
                e.with_context(|| "setgid(0) failed while entering sandbox")
                    .unwrap();
            }

            // The kernel marks /tmp/worker/overlay as MNT_LOCKED as a safety restriction due to
            // the use of user namespaces. pivot_root requires the new root not to be MNT_LOCKED
            // (the reason for which I don't quite understand), and the simplest way to fix that
            // is to bind-mount /tmp/worker/overlay onto itself.
            system::bind_mount_opt("/tmp/worker/overlay", "/tmp/worker/overlay", system::MS_REC)
                .with_context(|| "Failed to bind-mount /tmp/worker/overlay onto itself")
                .unwrap();

            // Change root
            std::env::set_current_dir("/tmp/worker/overlay")
                .with_context(|| "Failed to chdir to new root at /tmp/worker/overlay")
                .unwrap();
            nix::unistd::pivot_root(".", ".")
                .with_context(|| "Failed to pivot_root")
                .unwrap();
            system::umount_opt(".", system::MNT_DETACH)
                .with_context(|| "Failed to unmount self")
                .unwrap();
            std::env::set_current_dir("/")
                .with_context(|| "Failed to chdir to new root at /")
                .unwrap();

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
            let file = std::fs::File::open("/.sunwalker/env")
                .with_context(|| "Could not open /.sunwalker/env for reading")
                .unwrap();
            for line in std::io::BufReader::new(file).lines() {
                let line = line
                    .with_context(|| "Could not read from /.sunwalker/env")
                    .unwrap();
                let idx = line
                    .find('=')
                    .with_context(|| {
                        format!("'=' not found in a line of /.sunwalker/env: {}", line)
                    })
                    .unwrap();
                let (name, value) = line.split_at(idx);
                let value = &value[1..];
                std::env::set_var(name, value);
            }

            f();
        })?;

        let child_pid = child.get_pid();

        let mut wstatus: c_int = 0;
        let ret = unsafe { libc::waitpid(child_pid, &mut wstatus as *mut c_int, WUNTRACED) };
        if ret == -1 {
            Err(std::io::Error::last_os_error()).with_context(|| format!("waitpid() failed"))?;
        }
        if !libc::WIFSTOPPED(wstatus) {
            std::mem::forget(child); // we don't want to accidentally kill another process with the same PID
            bail!("Child process wasn't stopped by SIGSTOP, as expected");
        }

        // Fill uid/gid maps and switch to
        std::fs::write(
            format!("/proc/{}/uid_map", child_pid),
            format!("0 65534 1\n"),
        )
        .with_context(|| "Failed to write to child's uid_map")?;
        std::fs::write(format!("/proc/{}/setgroups", child_pid), "deny\n")
            .with_context(|| "Failed to write to child's setgroups")?;
        std::fs::write(
            format!("/proc/{}/gid_map", child_pid),
            format!("0 65534 1\n"),
        )
        .with_context(|| "Failed to write to child's gid_map")?;

        if unsafe { libc::kill(child_pid, SIGCONT) } != 0 {
            bail!("Failed to SIGCONT child process");
        }

        child.join().await
    }

    fn _remove(&mut self) -> Result<()> {
        if self.removed {
            return Ok(());
        }

        self.removed = true;

        // Unmount overlay recursively
        let file = std::fs::File::open("/proc/self/mounts")
            .with_context(|| "Could not open /proc/self/mounts for reading")?;
        let mut vec = Vec::new();
        for line in std::io::BufReader::new(file).lines() {
            let line = line?;
            let mut it = line.split(" ");
            it.next()
                .ok_or_else(|| anyhow!("Invalid format of /proc/self/mounts"))?;
            let target_path = it
                .next()
                .ok_or_else(|| anyhow!("Invalid format of /proc/self/mounts"))?;
            if target_path.starts_with("/tmp/worker/overlay") {
                vec.push(target_path.to_string());
            }
        }
        for path in vec.into_iter().rev() {
            system::umount(&path).with_context(|| format!("Failed to unmount {}", path))?;
        }

        // Remove directories
        std::fs::remove_dir_all("/tmp/worker/user-area")?;
        std::fs::remove_dir_all("/tmp/worker/work")?;
        std::fs::remove_dir_all("/tmp/worker/overlay")?;

        Ok(())
    }

    pub fn remove(mut self) -> Result<()> {
        self._remove()
    }
}

impl Drop for RootFS<'_> {
    fn drop(&mut self) {
        self._remove().unwrap();
    }
}
