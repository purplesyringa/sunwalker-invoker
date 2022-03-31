use crate::problem::{dependencies, problem};
use anyhow::{bail, Context, Result};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

pub struct ProblemStore {
    local_storage_path: PathBuf,
}

impl ProblemStore {
    pub fn new(local_storage_path: PathBuf) -> Result<ProblemStore> {
        let meta = std::fs::metadata(&local_storage_path)
            .with_context(|| "Problem store cache directory is inaccessible")?;
        if !meta.is_dir() {
            bail!("Problem store cache directory is not a directory");
        }
        Ok(ProblemStore { local_storage_path })
    }

    pub async fn load(&self, problem_id: &str) -> Result<Arc<problem::Problem>> {
        Ok(Arc::new(problem::Problem {
            dependency_dag: dependencies::DependencyDAG {
                dependents_of: Arc::new(HashMap::from([(1, vec![2]), (2, vec![3])])),
                disabled_tests: HashSet::new(),
            },
        }))
    }
}
