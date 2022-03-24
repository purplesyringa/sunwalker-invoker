use crate::{
    image::{config::Config, package},
    system,
};
use anyhow::{bail, Context, Result};
use libc::{MS_RDONLY, MS_REC};

pub struct ImageMounter {
    inc: u64,
}

#[derive(Debug)]
pub struct MountedImage {
    pub mountpoint: std::path::PathBuf,
    pub config: Config,
}

impl ImageMounter {
    pub fn new() -> ImageMounter {
        return ImageMounter { inc: 0 };
    }

    fn get_mountpoint(&mut self) -> std::path::PathBuf {
        self.inc += 1;
        std::path::PathBuf::from("/tmp/image-".to_owned() + &self.inc.to_string())
    }

    pub fn mount<P: AsRef<std::path::Path>>(
        &mut self,
        source_path: P,
        config: Config,
    ) -> Result<MountedImage> {
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
            system::bind_mount_opt(source_path, &mountpoint, MS_RDONLY | MS_REC)
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

        // Make sure that all the packages specified in the config are available
        for (package_name, _) in &config.packages {
            let mut package_path = mountpoint.clone();
            package_path.push(package_name);
            let meta = std::fs::metadata(package_path)
                .with_context(|| format!("The image does not provide package {}", package_name))?;
            if !meta.is_dir() {
                bail!("Package {} requires the image to contain a subdirectory named {}, but it is not a directory", package_name, package_name);
            }
        }

        Ok(MountedImage { mountpoint, config })
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
        system::umount(&self.mountpoint)
            .or_else(|_| system::umount_opt(&self.mountpoint, system::MNT_DETACH))
            .expect(&format!("Unmounting {:?} failed", self));
    }
}
