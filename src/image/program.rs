use crate::{
    errors,
    image::{package, sandbox},
};
use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Program {
    pub package: package::Package,
    pub prerequisites: Vec<String>,
    pub argv: Vec<String>,
}

impl Program {
    pub async fn run(&self, worker_space: &sandbox::WorkerSpace) -> Result<(), errors::Error> {
        let mut bound_files = Vec::new();
        for prerequisite in &self.prerequisites {
            bound_files.push((
                format!("/tmp/worker/artifacts/{}", prerequisite).into(),
                format!("/space/{}", prerequisite),
            ));
        }

        let rootfs = worker_space
            .make_rootfs(&self.package, bound_files)
            .map_err(|e| {
                errors::InvokerFailure(format!("Failed to make sandbox for running: {:?}", e))
            })?;

        // Enter the sandbox in another process
        rootfs
            .run_isolated(|| {
                std::env::set_current_dir("/space")
                    .with_context(|| "Failed to chdir to /space")
                    .unwrap();

                std::process::Command::new(&self.argv[0])
                    .args(&self.argv[1..])
                    .spawn()
                    .with_context(|| format!("Failed to spawn {:?}", self.argv))
                    .unwrap()
                    .wait()
                    .with_context(|| format!("Failed to get exit code of {:?}", self.argv))
                    .unwrap();
            })
            .await?;

        rootfs
            .remove()
            .map_err(|e| errors::InvokerFailure(format!("Failed to remove rootfs: {:?}", e)))?;

        Ok(())
    }
}
