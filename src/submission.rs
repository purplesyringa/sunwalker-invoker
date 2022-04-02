use crate::{
    errors,
    image::{language, program, sandbox},
    problem, worker,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::{mpsc, Mutex, RwLock};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    Compile,
    Test(u64),
}

#[derive(Debug)]
pub enum W2IMessage {
    CompilationResult(Result<program::Program, errors::Error>),
}

pub struct Submission {
    id: String,
    dependency_dag: RwLock<problem::dependencies::DependencyDAG>,
    language: language::Language,
    source_files: Vec<String>,
    program: RwLock<Option<program::Program>>,
    workers: RwLock<HashMap<u64, worker::Worker>>,
    cumulative_messages_tx: mpsc::UnboundedSender<W2IMessage>,
    cumulative_messages_rx: Mutex<mpsc::UnboundedReceiver<W2IMessage>>,
}

impl Submission {
    pub fn new(
        id: String,
        dependency_dag: problem::dependencies::DependencyDAG,
        language: language::Language,
    ) -> Result<Submission, errors::Error> {
        let root = format!("/tmp/submissions/{}", id);
        std::fs::create_dir(&root).map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to create a directory for submission {} at {}: {:?}",
                id, root, e
            ))
        })?;

        let (cumulative_messages_tx, cumulative_messages_rx) = mpsc::unbounded_channel();

        Ok(Submission {
            id,
            dependency_dag: RwLock::new(dependency_dag),
            language,
            source_files: Vec::new(),
            program: RwLock::new(None),
            workers: RwLock::new(HashMap::new()),
            cumulative_messages_tx,
            cumulative_messages_rx: Mutex::new(cumulative_messages_rx),
        })
    }

    pub fn add_source_file(&mut self, name: &str, content: &[u8]) -> Result<(), errors::Error> {
        let path = format!("/tmp/submissions/{}/{}", self.id, name);
        std::fs::write(&path, content).map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to write a source code file for submission {} at {}: {:?}",
                self.id, path, e
            ))
        })
    }

    async fn schedule_on_core(&self, core: u64, command: Command) -> Result<(), errors::Error> {
        let mut workers = self.workers.write().await;
        if !workers.contains_key(&core) {
            workers.insert(
                core,
                worker::Worker::new(
                    self.language.clone(),
                    self.source_files.clone(),
                    sandbox::SandboxConfig {
                        max_size_in_bytes: 8 * 1024 * 1024, // TODO: get from config
                        max_inodes: 1024,
                        core,
                    },
                    self.dependency_dag.read().await.clone(),
                    self.program.read().await.clone(),
                    self.cumulative_messages_tx.clone(),
                )?,
            );
        }

        let worker = workers.get(&core).unwrap();

        worker.push_command(command).await?;

        Ok(())
    }

    // Not abortable
    pub async fn compile_on_core(&self, core: u64) -> Result<(), errors::Error> {
        if self.program.read().await.is_some() {
            return Err(errors::ConductorFailure(
                "The submission is already compiled".to_string(),
            ));
        }
        self.schedule_on_core(core, Command::Compile).await?;
        let message = self
            .cumulative_messages_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| {
                errors::InvokerFailure(format!(
                    "Compilation result was not sent back to the submission object",
                ))
            })?;
        match message {
            W2IMessage::CompilationResult(Ok(program)) => {
                *self.program.write().await = Some(program);
                Ok(())
            }
            W2IMessage::CompilationResult(Err(e)) => Err(e),
            _ => Err(errors::InvokerFailure(format!(
                "An unexpected message was received while waiting for compilation result: {:?}",
                message
            ))),
        }
    }

    pub async fn schdule_test_on_core(&self, core: u64, test: u64) -> Result<(), errors::Error> {
        if self.program.read().await.is_none() {
            return Err(errors::ConductorFailure(
                "Cannot judge submission before the program is built".to_string(),
            ));
        }
        self.schedule_on_core(core, Command::Test(test)).await
    }

    pub async fn add_failed_tests(&self, tests: &[u64]) -> Result<(), errors::Error> {
        {
            let mut dependency_dag = self.dependency_dag.write().await;
            for test in tests {
                dependency_dag.fail_test(*test);
            }
        }
        for (_, worker) in self.workers.read().await.iter() {
            worker.add_failed_tests(Vec::from(tests)).await?;
        }
        Ok(())
    }

    pub async fn finalize(&mut self) -> Result<(), errors::Error> {
        Ok(())
    }
}
