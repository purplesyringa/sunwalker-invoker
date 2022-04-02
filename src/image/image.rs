use crate::{
    image::{config, language, package},
    system,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

// Duplicating an owned image via serializing or cloning it is unsafe
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Image {
    pub mountpoint: std::path::PathBuf,
    pub config: config::Config,
    pub language_to_package_name: HashMap<String, String>,
    // owned == true: Automatically unmounted on Drop.
    // owned == false: Assumes the mountpoint never dies and doesn't unmount it itself. The former
    // is guaranteed if the mount namespace is unshared.
    // owned: bool,
}

impl Image {
    pub fn has_package(&self, package: &str) -> bool {
        let mut path = self.mountpoint.clone();
        path.push(package);
        path.exists()
    }

    pub fn has_language(&self, name: &str) -> bool {
        self.language_to_package_name.contains_key(name)
    }

    pub fn get_language(image: Arc<Image>, name: String) -> Result<language::Language> {
        let package_name = image
            .language_to_package_name
            .get(&name)
            .with_context(|| format!("The image does not provide language {}", name))?
            .clone();
        package::Package::new(image, package_name)?.get_language(&name)
    }

    // pub fn clone_disowned(&self) -> Image {
    //     Image {
    //         mountpoint: self.mountpoint.clone(),
    //         config: self.config.clone(),
    //         language_to_package_name: self.language_to_package_name.clone(),
    //         owned: false,
    //     }
    // }
}

impl Drop for Image {
    fn drop(&mut self) {
        // if self.owned {
        // TODO: add logging
        system::umount(&self.mountpoint)
            .or_else(|_| system::umount_opt(&self.mountpoint, system::MNT_DETACH))
            .expect(&format!("Unmounting {:?} failed", self));
        // }
    }
}
