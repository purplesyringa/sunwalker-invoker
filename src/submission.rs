use crate::{
    errors,
    image::{language, program},
    problem::{problem, verdict},
    worker,
};
use itertools::Itertools;
use multiprocessing::Object;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Debug, Object)]
pub enum Command {
    Compile(String),
    Test(u64),
    Finalize,
}

pub struct Submission {
    pub id: String,
    instantiated_dependency_dag: RwLock<problem::InstantiatedDependencyDAG>,
    language: language::Language,
    source_files: Vec<String>,
    program: RwLock<Option<program::Program>>,
    workers: RwLock<HashMap<u64, Arc<RwLock<worker::Worker>>>>,
    problem_revision: Arc<problem::ProblemRevision>,
}

impl Submission {
    pub fn new(
        id: String,
        problem_revision: Arc<problem::ProblemRevision>,
        language: language::Language,
    ) -> Result<Submission, errors::Error> {
        let root = format!("/tmp/submissions/{}", id);
        std::fs::create_dir(&root).map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to create a directory for submission {} at {}: {:?}",
                id, root, e
            ))
        })?;

        Ok(Submission {
            id,
            instantiated_dependency_dag: RwLock::new(
                problem_revision.dependency_dag.clone().instantiate(),
            ),
            language,
            source_files: Vec::new(),
            program: RwLock::new(None),
            workers: RwLock::new(HashMap::new()),
            problem_revision,
        })
    }

    pub fn add_source_file(&mut self, name: &str, content: &[u8]) -> Result<(), errors::Error> {
        let path = format!("/tmp/submissions/{}/{}", self.id, name);
        std::fs::write(&path, content).map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to write a source code file for submission {} at {}: {:?}",
                self.id, path, e
            ))
        })?;
        self.source_files.push(path);
        Ok(())
    }

    async fn execute_on_core(
        &self,
        core: u64,
        command: Command,
    ) -> Result<worker::W2IMessage, errors::Error> {
        use std::collections::hash_map::Entry;

        let mut workers = self.workers.write().await;
        let worker = match workers.entry(core) {
            Entry::Occupied(occupied) => occupied.get().clone(),
            Entry::Vacant(vacant) => vacant
                .insert(Arc::new(RwLock::new(
                    worker::Worker::new(
                        self.language.clone(),
                        self.source_files.clone(),
                        core,
                        self.instantiated_dependency_dag.read().await.clone(),
                        self.program.read().await.clone(),
                        self.problem_revision.strategy_factory.clone(),
                        self.problem_revision.data.clone(),
                    )
                    .await?,
                )))
                .clone(),
        };
        drop(workers);

        let worker = worker.read().await;
        worker.execute_command(command).await
    }

    pub async fn compile_on_core(&self, core: u64) -> Result<String, errors::Error> {
        if self.program.read().await.is_some() {
            return Err(errors::ConductorFailure(
                "The submission is already compiled".to_string(),
            ));
        }

        let response = self
            .execute_on_core(core, Command::Compile(format!("judge-{}", self.id)))
            .await?;
        match response {
            worker::W2IMessage::CompilationResult(program, log) => {
                *self.program.write().await = Some(program);
                Ok(log)
            }
            worker::W2IMessage::Failure(e) => Err(e),
            _ => Err(errors::InvokerFailure(format!(
                "Unexpected response to compilation request: {:?}",
                response
            ))),
        }
    }

    pub async fn test_on_core(
        &self,
        core: u64,
        test: u64,
    ) -> Result<verdict::TestJudgementResult, errors::Error> {
        if self.program.read().await.is_none() {
            return Err(errors::ConductorFailure(
                "Cannot judge submission before the program is built".to_string(),
            ));
        }

        let response = self.execute_on_core(core, Command::Test(test)).await?;
        match response {
            worker::W2IMessage::TestResult(result) => Ok(result),
            worker::W2IMessage::Failure(e) => Err(e),
            _ => Err(errors::InvokerFailure(format!(
                "Unexpected response to judgement request: {:?}",
                response
            ))),
        }
    }

    pub async fn add_failed_tests(&self, tests: &[u64]) -> Result<(), errors::Error> {
        {
            let mut instantiated_dependency_dag = self.instantiated_dependency_dag.write().await;
            for test in tests {
                instantiated_dependency_dag.fail_test(*test);
            }
        }
        for (_, worker) in self.workers.read().await.iter() {
            worker
                .read()
                .await
                .add_failed_tests(Vec::from(tests))
                .await?;
        }
        Ok(())
    }

    pub async fn finalize(&self) -> Result<(), errors::Error> {
        if let Some(program) = self.program.write().await.take() {
            program.remove()?;
        }
        let mut workers = self.workers.write().await;

        let mut errors = futures::future::join_all(
            workers
                .iter_mut()
                .map(async move |(_, worker)| worker.write().await.finalize().await),
        )
        .await
        .into_iter()
        .filter_map(|res| res.err())
        .peekable();

        if errors.peek().is_none() {
            Ok(())
        } else {
            Err(errors::InvokerFailure(format!(
                "Failed to finalize submission: {}",
                errors.map(|e| format!("{:?}", e)).join(", ")
            )))
        }
    }
}
