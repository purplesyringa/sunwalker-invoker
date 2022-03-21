use crate::image::package;
use anyhow::{bail, Context, Result};
use libc::{MS_BIND, MS_RDONLY, MS_REC};

pub struct ImageMounter {
    inc: u64,
}

#[derive(Debug)]
pub struct MountedImage {
    pub mountpoint: std::path::PathBuf,
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

    pub fn get_package<'a>(&'a self, name: &'a str) -> Result<package::Package<'a>> {
        package::Package::new(self, name)
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
