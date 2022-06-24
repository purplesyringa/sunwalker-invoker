use crate::{
    errors,
    image::{image, package, sandbox},
};
use multiprocessing::Object;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Debug, Object)]
pub struct Program {
    pub package: package::Package,
    pub prerequisites: Vec<String>,
    pub argv: Vec<String>,
    pub artifacts_path: PathBuf,
}

pub struct InvocableProgram {
    pub program: Program,
    pub rootfs: sandbox::RootFS,
    pub namespace: sandbox::Namespace,
}

#[derive(Clone, Object, Serialize, Deserialize)]
pub struct CachedProgram {
    pub package: String,
    pub prerequisites: Vec<String>,
    pub argv: Vec<String>,
}

impl Program {
    pub fn from_cached_program(
        program: CachedProgram,
        path: &Path,
        image: Arc<image::Image>,
    ) -> Result<Self, errors::Error> {
        for prerequisite in program.prerequisites.iter() {
            let prerequisite_path = path.join("artifacts").join(prerequisite);
            let metadata = std::fs::metadata(&prerequisite_path).map_err(|e| {
                errors::ConfigurationFailure(format!(
                    "Prerequisite {} was specified, but could not be stat'ed at path {:?}: {:?}",
                    prerequisite, prerequisite_path, e
                ))
            })?;
            if !metadata.is_file() {
                return Err(errors::ConfigurationFailure(format!(
                    "Prerequisite {} was specified, but {:?} is not a regular file",
                    prerequisite, prerequisite_path
                )));
            }
        }
        Ok(Self {
            package: package::Package::new(image, program.package.clone()).map_err(|e| {
                errors::ConfigurationFailure(format!(
                    "The current image faled to provide package {}, which was specified in \
                     program.cfg at {:?}: {:?}",
                    program.package, path, e
                ))
            })?,
            prerequisites: program.prerequisites,
            argv: program.argv,
            artifacts_path: path.join("artifacts"),
        })
    }

    pub async fn into_invocable(self, id: String) -> Result<InvocableProgram, errors::Error> {
        let mut bound_files = Vec::new();
        for prerequisite in &self.prerequisites {
            bound_files.push((
                self.artifacts_path.join(prerequisite),
                format!("/space/{}", prerequisite),
            ));
        }
        let rootfs = sandbox::make_rootfs(
            &self.package,
            bound_files,
            sandbox::DiskQuotas {
                space: 32 * 1024 * 1024, // TODO: make this configurable
                max_inodes: 1024,
            },
            id.clone(),
        )
        .map_err(|e| {
            errors::InvokerFailure(format!("Failed to make rootfs for running: {:?}", e))
        })?;

        let namespace = sandbox::make_namespace(id).await.map_err(|e| {
            errors::InvokerFailure(format!("Failed to make namespace for running: {:?}", e))
        })?;

        Ok(InvocableProgram {
            program: self,
            rootfs,
            namespace,
        })
    }

    pub fn remove(self) -> Result<(), errors::Error> {
        std::fs::remove_dir_all(&self.artifacts_path).map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to remove program artifacts at {:?}: {:?}",
                self.artifacts_path, e
            ))
        })
    }
}
