use crate::image::{image, language};
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Package {
    pub image: Arc<image::Image>,
    pub name: String,
}

impl Package {
    pub fn new(image: Arc<image::Image>, name: String) -> Result<Package> {
        if !image.has_package(name.as_ref()) {
            bail!("Image {:?} does not contain package {}", image, name);
        }
        Ok(Package { image, name })
    }

    pub fn get_language(&self, language_name: &str) -> Result<language::Language> {
        language::Language::new(self.clone(), language_name)
    }
}
