use crate::{client, errors, errors::ToResult, problem::problem};
use anyhow::{bail, Context};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub struct ProblemStore {
    local_storage_path: PathBuf,
    locks: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    communicator: Arc<client::Communicator>,
}

impl ProblemStore {
    pub fn new(
        local_storage_path: PathBuf,
        communicator: Arc<client::Communicator>,
    ) -> anyhow::Result<ProblemStore> {
        let meta = std::fs::metadata(&local_storage_path)
            .with_context(|| "Problem store cache directory is inaccessible")?;
        if !meta.is_dir() {
            bail!("Problem store cache directory is not a directory");
        }
        Ok(ProblemStore {
            local_storage_path,
            locks: RwLock::new(HashMap::new()),
            communicator,
        })
    }

    pub async fn load_revision(
        &self,
        problem_id: String,
        revision_id: String,
    ) -> Result<Arc<problem::ProblemRevision>, errors::Error> {
        let topic = format!("problems/{problem_id}/{revision_id}");
        let root_path = self.local_storage_path.join(problem_id).join(revision_id);

        let mutex = self
            .locks
            .write()
            .await
            .entry(topic.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        {
            let _guard = mutex.lock().await;

            if !root_path.exists() {
                std::fs::create_dir_all(&root_path)
                    .with_context_invoker(|| format!("Failed to create directory {root_path:?}"))?;
            }

            if !root_path.join(".ready").exists() {
                self.communicator
                    .download_archive(&topic, root_path.as_ref())
                    .await
                    .with_context_invoker(|| format!("Failed to load archive for topic {topic}"))?;
            }
        }

        Ok(Arc::new(problem::ProblemRevision::load_from_cache(
            &root_path,
        )?))
    }
}
