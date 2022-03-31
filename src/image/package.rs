use crate::image::{language, mount};
use anyhow::{bail, Result};
use std::sync::Arc;

#[derive(Clone)]
pub struct Package {
    pub image: Arc<mount::MountedImage>,
    pub name: String,
}

impl Package {
    pub fn new(image: Arc<mount::MountedImage>, name: String) -> Result<Package> {
        if !image.has_package(name.as_ref()) {
            bail!("Image {:?} does not contain package {}", image, name);
        }
        Ok(Package { image, name })
    }

    pub fn get_language(&self, language_name: &str) -> Result<language::Language> {
        language::Language::new(self.clone(), language_name)
    }
}
