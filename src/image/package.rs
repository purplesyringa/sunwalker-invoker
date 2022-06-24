use crate::{
    errors,
    image::{image, language},
};
use multiprocessing::Object;
use std::sync::Arc;

#[derive(Clone, Debug, Object)]
pub struct Package {
    pub image: Arc<image::Image>,
    pub name: String,
}

impl Package {
    pub fn new(image: Arc<image::Image>, name: String) -> Result<Package, errors::Error> {
        if image.has_package(name.as_ref()) {
            Ok(Package { image, name })
        } else {
            Err(errors::ConfigurationFailure(format!(
                "Image {:?} does not contain package {}",
                image.mountpoint, name
            )))
        }
    }

    pub fn get_language(&self, language_name: &str) -> Result<language::Language, errors::Error> {
        language::Language::new(self.clone(), language_name)
    }
}
