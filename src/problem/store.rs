use crate::{communicator, errors, errors::ToResult, problem::problem};
use anyhow::{bail, Context};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct ProblemStore {
    local_storage_path: PathBuf,
    problems: Mutex<HashMap<String, Arc<Mutex<Option<Arc<problem::ProblemRevision>>>>>>,
    communicator: Arc<communicator::Communicator>,
}

impl ProblemStore {
    pub fn new(
        local_storage_path: PathBuf,
        communicator: Arc<communicator::Communicator>,
    ) -> anyhow::Result<ProblemStore> {
        let meta = std::fs::metadata(&local_storage_path)
            .with_context(|| "Problem store cache directory is inaccessible")?;
        if !meta.is_dir() {
            bail!("Problem store cache directory is not a directory");
        }
        Ok(ProblemStore {
            local_storage_path,
            problems: Mutex::new(HashMap::new()),
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
            .problems
            .lock()
            .await
            .entry(topic.clone())
            .or_insert_with(|| Arc::new(Mutex::new(None)))
            .clone();

        let mut guard = mutex.lock().await;

        if guard.is_none() {
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

            *guard = Some(Arc::new(problem::ProblemRevision::load_from_cache(
                &root_path,
            )?));
        }

        Ok(guard.as_ref().unwrap().clone())
    }
}
