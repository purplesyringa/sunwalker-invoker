use crate::{
    errors,
    image::{config, language, package},
};
use multiprocessing::Object;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Object)]
pub struct Image {
    pub mountpoint: std::path::PathBuf,
    pub config: config::Config,
    pub language_to_package_name: HashMap<String, String>,
}

impl Image {
    pub fn has_package(&self, package: &str) -> bool {
        self.mountpoint.join(package).exists()
    }

    pub fn has_language(&self, name: &str) -> bool {
        self.language_to_package_name.contains_key(name)
    }

    pub fn get_language(
        image: Arc<Image>,
        name: String,
    ) -> Result<language::Language, errors::Error> {
        let package_name = image
            .language_to_package_name
            .get(&name)
            .ok_or_else(|| {
                errors::UserFailure(format!("The image does not provide language {}", name))
            })?
            .clone();
        package::Package::new(image, package_name)?.get_language(&name)
    }
}

// Leaks? Who cares? At least it doesn't crash magnificently.
// impl Drop for Image {
//     fn drop(&mut self) {
//         system::umount(&self.mountpoint)
//             .or_else(|_| system::umount_opt(&self.mountpoint, system::MNT_DETACH))
//             .expect(&format!(
//                 "Failed to unmount image at {:?}",
//                 self.mountpoint
//             ));
//     }
// }
