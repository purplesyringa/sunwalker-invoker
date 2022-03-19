use libc::{c_char, MS_BIND, MS_RDONLY, MS_REC, MNT_DETACH};
use std::os::unix::ffi::OsStrExt;
use anyhow::{Result, Context, bail};


fn as_null_terminated<S: AsRef<std::ffi::OsStr>>(s: S) -> Vec<u8> {
    let mut result = Vec::from(s.as_ref().as_bytes());
    result.push(0);
    result
}


pub struct ImageMounter {
    inc: u64
}


#[derive(Debug)]
pub struct MountedImage {
    mountpoint: std::path::PathBuf
}


impl ImageMounter {
    pub fn new() -> ImageMounter {
        return ImageMounter {
            inc: 0
        };
    }


    fn get_mountpoint(&mut self) -> std::path::PathBuf {
        self.inc += 1;
        std::path::PathBuf::from("/tmp/sunwalker-".to_owned() + &self.inc.to_string())
    }


    pub fn mount<P: AsRef<std::path::Path>>(&mut self, source_path: P) -> Result<MountedImage> {
        let attr = std::fs::metadata(&source_path)
            .with_context(|| format!("Cannot get matadata of {:?} (does the file exist?)", source_path.as_ref()))?;

        let mountpoint = self.get_mountpoint();
        std::fs::create_dir(&mountpoint)
            .with_context(|| format!("Unable to create temporary mountpoint at {:?}", &mountpoint))?;

        let file_type = attr.file_type();

        let source_path_c = as_null_terminated(source_path.as_ref());
        let mountpoint_c = as_null_terminated(&mountpoint);

        let flags: u64;
        let mount_type: *const c_char;
        if file_type.is_dir() {
            // Bind-mount
            flags = MS_BIND | MS_RDONLY | MS_REC;
            mount_type = std::ptr::null();
        } else if file_type.is_file() {
            // Mount as squashfs image

            // TODO: this code allows to use normal mount for squashfs, but this requires root access;
            // we use squashfuse instead for now.
            // flags = MS_RDONLY;
            // mount_type = cstr!("squashfs");

            let output = std::process::Command::new("squashfuse")
                .arg(&source_path.as_ref())
                .arg(&mountpoint)
                .output()
                .with_context(|| format!("Failed to start squashfuse to mount image {:?}", source_path.as_ref()))?;

            if !output.status.success() {
                bail!(format!("squashfuse for {:?} -> {:?} returned {}", source_path.as_ref(), &mountpoint, output.status));
            }

            return Ok(MountedImage { mountpoint });
        } else {
            bail!("Cannot mount image of unknown file type (neither file, nor directory)");
        }

        unsafe {
            if libc::mount(
                source_path_c.as_slice() as *const [u8] as *const c_char,
                mountpoint_c.as_slice() as *const [u8] as *const c_char,
                mount_type,
                flags,
                std::ptr::null()
            ) != 0 {
                return Err(
                    anyhow::Error::from(std::io::Error::last_os_error())
                        .context("mount() failed")
                );
            }
        }

        Ok(MountedImage { mountpoint })
    }
}


impl Drop for MountedImage {
    fn drop(&mut self) {
        unsafe {
            let mountpoint_c = as_null_terminated(&self.mountpoint);
            let ptr = mountpoint_c.as_slice() as *const [u8] as *const c_char;
            if libc::umount2(ptr, 0) != 0 {
                if libc::umount2(ptr, MNT_DETACH) != 0 {
                    // TODO: replace with logging
                    panic!("Unmounting {:?} failed", self);
                }
            }
        }
    }
}
