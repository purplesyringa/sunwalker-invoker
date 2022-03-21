use anyhow::{bail, Context, Result};
use libc::{CLONE_NEWNS, MS_BIND, MS_RDONLY, MS_REC};
use std::io::BufRead;

pub struct ImageMounter {
    inc: u64,
}

#[derive(Debug)]
pub struct MountedImage {
    mountpoint: std::path::PathBuf,
}

pub struct SandboxDiskConfig {
    pub max_size_in_bytes: u64,
    pub max_inodes: u64,
}

impl ImageMounter {
    pub fn new() -> ImageMounter {
        return ImageMounter { inc: 0 };
    }

    fn get_mountpoint(&mut self) -> std::path::PathBuf {
        self.inc += 1;
        std::path::PathBuf::from("/tmp/image-".to_owned() + &self.inc.to_string())
    }

    pub fn mount<P: AsRef<std::path::Path>>(&mut self, source_path: P) -> Result<MountedImage> {
        let attr = std::fs::metadata(&source_path).with_context(|| {
            format!(
                "Cannot get matadata of {:?} (does the file exist?)",
                source_path.as_ref()
            )
        })?;

        let mountpoint = self.get_mountpoint();
        std::fs::create_dir(&mountpoint).with_context(|| {
            format!("Unable to create temporary mountpoint at {:?}", &mountpoint)
        })?;

        let file_type = attr.file_type();

        if file_type.is_dir() {
            // Bind-mount
            sys_mount::Mount::builder()
                .flags(unsafe {
                    sys_mount::MountFlags::from_bits_unchecked(MS_BIND | MS_RDONLY | MS_REC)
                })
                .mount(source_path, &mountpoint)
                .with_context(|| "Bind-mounting image failed")?;
        } else if file_type.is_file() {
            // Mount as squashfs image
            sys_mount::Mount::builder()
                .flags(sys_mount::MountFlags::RDONLY)
                .fstype("squashfs")
                .mount(source_path, &mountpoint)
                .with_context(|| "Mounting squashfs image failed")?;

            // NOTE: the code below allows to use squashfuse instead of the kernel driver. Is it useful?
            // let output = std::process::Command::new("squashfuse")
            //     .arg(&source_path.as_ref())
            //     .arg(&mountpoint)
            //     .output()
            //     .with_context(|| {
            //         format!(
            //             "Failed to start squashfuse to mount image {:?}",
            //             source_path.as_ref()
            //         )
            //     })?;

            // if !output.status.success() {
            //     bail!(format!(
            //         "squashfuse for {:?} -> {:?} returned {}",
            //         source_path.as_ref(),
            //         &mountpoint,
            //         output.status
            //     ));
            // }
        } else {
            bail!("Cannot mount image of unknown file type (neither file, nor directory)");
        }

        Ok(MountedImage { mountpoint })
    }
}

impl MountedImage {
    pub fn has_package(&self, package: &str) -> bool {
        let mut path = self.mountpoint.clone();
        path.push(package);
        path.exists()
    }

    pub fn enter(&self, package: &str, config: &SandboxDiskConfig) -> Result<()> {
        if !self.has_package(package) {
            bail!("This image does not contain package {}", package);
        }

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
                    self.mountpoint
                        .to_str()
                        .expect("Mountpoint must be a string"),
                    package
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

impl Drop for MountedImage {
    fn drop(&mut self) {
        // TODO: add logging
        sys_mount::unmount(&self.mountpoint, sys_mount::UnmountFlags::empty())
            .or_else(|_| sys_mount::unmount(&self.mountpoint, sys_mount::UnmountFlags::DETACH))
            .expect(&format!("Unmounting {:?} failed", self));
    }
}
