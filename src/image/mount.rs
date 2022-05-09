use crate::{
    image::{config, image},
    system,
};
use anyhow::{bail, Context, Result};
use libc::{MS_RDONLY, MS_REC};
use std::collections::HashMap;

pub struct ImageMounter {
    inc: u64,
}

impl ImageMounter {
    pub fn new() -> ImageMounter {
        return ImageMounter { inc: 0 };
    }

    fn get_mountpoint(&mut self) -> std::path::PathBuf {
        self.inc += 1;
        std::path::PathBuf::from("/tmp/image-".to_owned() + &self.inc.to_string())
    }

    pub fn mount<'a, P: AsRef<std::path::Path>>(
        &mut self,
        source_path: P,
        config: config::Config,
    ) -> Result<image::Image> {
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
            // TODO: unmount loop device on exit
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
        let mut language_to_package_name = HashMap::new();
        for (package_name, package) in &config.packages {
            // TODO: uncomment this
            // let mut package_path = mountpoint.clone();
            // package_path.push(package_name);
            // let meta = std::fs::metadata(package_path)
            //     .with_context(|| format!("The image does not provide package {}", package_name))?;
            // if !meta.is_dir() {
            //     bail!("Package {} requires the image to contain a subdirectory named {}, but it is not a directory", package_name, package_name);
            // }
            for (lang_name, _) in &package.languages {
                if let Some(old_package_name) =
                    language_to_package_name.insert(lang_name.clone(), package_name.clone())
                {
                    bail!(
                        "Collision detected: language {} is provided by two packages: {} and {}",
                        lang_name,
                        old_package_name,
                        package_name
                    );
                }
            }
        }

        Ok(image::Image {
            mountpoint,
            config,
            language_to_package_name,
        })
    }
}
