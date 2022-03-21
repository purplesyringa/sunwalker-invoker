use crate::image::mount;
use anyhow::{bail, Context, Result};
use libc::CLONE_NEWNS;
use std::io::BufRead;

pub struct SandboxDiskConfig {
    pub max_size_in_bytes: u64,
    pub max_inodes: u64,
}

pub struct Package<'a> {
    image: &'a mount::MountedImage,
    name: &'a str,
}

impl<'a> Package<'a> {
    pub fn new(image: &'a mount::MountedImage, name: &'a str) -> Result<Package<'a>> {
        if !image.has_package(name.as_ref()) {
            bail!("Image {:?} does not contain package {}", image, name);
        }
        Ok(Package { image, name })
    }

    pub fn enter(&self, config: &SandboxDiskConfig) -> Result<()> {
        // Unshare namespaces
        unsafe {
            if libc::unshare(CLONE_NEWNS) != 0 {
                bail!("Could not unshare mount namespace");
            }
        }

        // Create per-worker tmpfs
        sys_mount::Mount::builder()
            .fstype("tmpfs")
            .data(
                format!(
                    "size={},nr_inodes={}",
                    config.max_size_in_bytes, config.max_inodes
                )
                .as_ref(),
            )
            .mount("tmpfs", "/tmp/worker")
            .with_context(|| "Mounting tmpfs on /tmp/worker failed")?;

        std::fs::create_dir("/tmp/worker/user-area")?;
        std::fs::create_dir("/tmp/worker/work")?;
        std::fs::create_dir("/tmp/worker/overlay")?;

        // Mount overlay
        sys_mount::Mount::builder()
            .fstype("overlay")
            .data(
                format!(
                    "lowerdir={}/{},upperdir=/tmp/worker/user-area,workdir=/tmp/worker/work",
                    self.image
                        .mountpoint
                        .to_str()
                        .expect("Mountpoint must be a string"),
                    self.name
                )
                .as_ref(),
            )
            .mount("overlay", "/tmp/worker/overlay")
            .with_context(|| "Failed to mount overlay")?;

        // Mount /dev on overlay
        std::fs::create_dir("/tmp/worker/overlay/dev")
            .with_context(|| "Failed to create .../dev")?;
        sys_mount::Mount::builder()
            .flags(sys_mount::MountFlags::BIND)
            .mount("/tmp/dev", "/tmp/worker/overlay/dev")
            .with_context(|| "Failed to mount /dev on overlay")?;

        // Change root
        std::fs::create_dir("/tmp/worker/overlay/old-root")
            .with_context(|| "Failed to create .../old-root")?;
        std::env::set_current_dir("/tmp/worker/overlay")
            .with_context(|| "Failed to chdir to new root")?;
        nix::unistd::pivot_root("/tmp/worker/overlay", "/tmp/worker/overlay/old-root")
            .with_context(|| "Failed to pivot_root")?;
        sys_mount::unmount("/old-root", sys_mount::UnmountFlags::DETACH)
            .with_context(|| "Failed to unmount /old-root")?;

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
            .with_context(|| "Could not open /.sunwalker/env for reading")?;
        for line in std::io::BufReader::new(file).lines() {
            let line = line.with_context(|| "Could not read from /.sunwalker/env")?;
            let idx = line
                .find('=')
                .with_context(|| format!("'=' not found in a line of /.sunwalker/env: {}", line))?;
            let (name, value) = line.split_at(idx);
            let value = &value[1..];
            std::env::set_var(name, value);
        }

        Ok(())
    }
}
