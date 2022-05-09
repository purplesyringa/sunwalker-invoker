use crate::{
    errors,
    image::{package, sandbox},
};
use multiprocessing::{Bind, Object};

#[derive(Clone, Debug, Object)]
pub struct Program {
    pub package: package::Package,
    pub prerequisites: Vec<String>,
    pub argv: Vec<String>,
    pub build_id: String,
}

impl Program {
    pub fn make_rootfs(
        &self,
        worker_space: &sandbox::WorkerSpace,
    ) -> Result<sandbox::RootFS, errors::Error> {
        let mut bound_files = Vec::new();
        for prerequisite in &self.prerequisites {
            bound_files.push((
                format!("/tmp/artifacts/{}/{}", self.build_id, prerequisite).into(),
                format!("/space/{}", prerequisite),
            ));
        }
        worker_space
            .make_rootfs(&self.package, bound_files, "run".to_string())
            .map_err(|e| {
                errors::InvokerFailure(format!("Failed to make sandbox for running: {:?}", e))
            })
    }

    pub async fn run(
        &self,
        rootfs: &mut sandbox::RootFS,
        ns: &mut sandbox::Namespace,
    ) -> Result<(), errors::Error> {
        sandbox::run_isolated(Box::new(start.bind(self.argv.clone())), rootfs, ns).await
    }

    pub fn remove(self) -> Result<(), errors::Error> {
        let path = format!("/tmp/artifacts/{}", self.build_id);
        std::fs::remove_dir_all(&path).map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to remove program artifacts at {}: {:?}",
                path, e
            ))
        })
    }
}

#[multiprocessing::entrypoint]
fn start(argv: Vec<String>) -> Result<(), errors::Error> {
    std::env::set_current_dir("/space")
        .map_err(|e| errors::InvokerFailure(format!("Failed to chdir to /space: {:?}", e)))?;

    std::process::Command::new(&argv[0])
        .args(&argv[1..])
        .spawn()
        .map_err(|e| errors::InvokerFailure(format!("Failed to spawn {:?}: {:?}", argv, e)))?
        .wait()
        .map_err(|e| {
            errors::InvokerFailure(format!("Failed to get exit code of {:?}: {:?}", argv, e))
        })?;

    Ok(())
}
