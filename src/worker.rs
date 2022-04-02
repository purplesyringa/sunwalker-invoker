use crate::{
    // entry::entrypoint,
    errors,
    image::{language, program, sandbox},
    ipc,
    problem,
    process,
    submission,
};
use futures::future::{AbortHandle, Abortable};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};

#[derive(Serialize, Deserialize)]
enum I2WUrgentCommand {
    AddFailedTests(Vec<u64>),
}

#[derive(Serialize, Deserialize)]
enum W2INotification {
    CompilationResult(Result<program::Program, errors::Error>),
}

pub struct Worker {
    tx_i2w_command: Mutex<ipc::Sender<submission::Command>>,
    tx_i2w_urgent: Mutex<ipc::Sender<I2WUrgentCommand>>,
    detached: Arc<DetachedState>,
}

struct DetachedState {
    rx_w2i: Mutex<ipc::Receiver<W2INotification>>,
    event_sink: mpsc::UnboundedSender<submission::W2IMessage>,
}

struct Subprocess {
    rx_i2w_command: Mutex<ipc::Receiver<submission::Command>>,
    rx_i2w_urgent: Mutex<ipc::Receiver<I2WUrgentCommand>>,
    tx_w2i: Mutex<ipc::Sender<W2INotification>>,
    worker_space: sandbox::WorkerSpace,
    current_command: Mutex<Option<(submission::Command, AbortHandle)>>,
    program: RwLock<Option<program::Program>>,
    language: language::Language,
    source_files: Vec<String>,
    dependency_dag: RwLock<problem::dependencies::DependencyDAG>,
}

#[derive(Serialize, Deserialize)]
struct InitializationInfo {
    rx_i2w_command: ipc::Receiver<submission::Command>,
    rx_i2w_urgent: ipc::Receiver<I2WUrgentCommand>,
    tx_w2i: ipc::Sender<W2INotification>,
    language: language::Language,
    source_files: Vec<String>,
    sandbox_config: sandbox::SandboxConfig,
    dependency_dag: problem::dependencies::DependencyDAG,
    program: Option<program::Program>,
}

impl Worker {
    pub fn new(
        language: language::Language,
        source_files: Vec<String>,
        sandbox_config: sandbox::SandboxConfig,
        dependency_dag: problem::dependencies::DependencyDAG,
        program: Option<program::Program>,
        event_sink: mpsc::UnboundedSender<submission::W2IMessage>,
    ) -> Result<Worker, errors::Error> {
        let (tx_i2w_command, rx_i2w_command) = ipc::channel(ipc::Direction::ParentToChild)
            .map_err(|e| {
                errors::InvokerFailure(format!("Failed to create an IPC channel: {:?}", e))
            })?;

        let (tx_i2w_urgent, rx_i2w_urgent) =
            ipc::channel(ipc::Direction::ParentToChild).map_err(|e| {
                errors::InvokerFailure(format!("Failed to create an IPC channel: {:?}", e))
            })?;

        let (tx_w2i, rx_w2i) = ipc::channel(ipc::Direction::ChildToParent).map_err(|e| {
            errors::InvokerFailure(format!("Failed to create an IPC channel: {:?}", e))
        })?;

        let init = InitializationInfo {
            rx_i2w_command,
            rx_i2w_urgent,
            tx_w2i,
            language,
            source_files,
            sandbox_config,
            dependency_dag,
            program,
        };

        let init = rmp_serde::to_vec(&init).map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to serialize the initialization message to the worker subprocess: {:?}",
                e
            ))
        })?;

        process::spawn(move || {
            Subprocess::main(init).unwrap();
        })
        .map_err(|e| {
            errors::InvokerFailure(format!("Failed to spawn a worker subprocess: {:?}", e))
        })?;

        let worker = Worker {
            tx_i2w_command: Mutex::new(tx_i2w_command),
            tx_i2w_urgent: Mutex::new(tx_i2w_urgent),
            detached: Arc::new(DetachedState {
                rx_w2i: Mutex::new(rx_w2i),
                event_sink,
            }),
        };

        tokio::spawn(work_detached(worker.detached.clone()));

        Ok(worker)
    }

    pub async fn push_command(&self, command: submission::Command) -> Result<(), errors::Error> {
        if let Err(_) = self.tx_i2w_command.lock().await.send(command).await {
            Err(errors::InvokerFailure(
                "Failed to send command to the worker--it was possibly terminated".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    pub async fn add_failed_tests(&self, tests: Vec<u64>) -> Result<(), errors::Error> {
        self.tx_i2w_urgent
            .lock()
            .await
            .send(I2WUrgentCommand::AddFailedTests(tests))
            .await
            .map_err(|e| {
                errors::InvokerFailure(format!(
                    "Failed to notify the worker subprocess about failed tests: {:?}",
                    e
                ))
            })
    }
}

async fn work_detached(state: Arc<DetachedState>) -> Result<(), errors::Error> {
    while let Some(notification) = state.rx_w2i.lock().await.recv().await.map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to read notification from worker subprocess: {:?}",
            e
        ))
    })? {
        match notification {
            W2INotification::CompilationResult(program) => state
                .event_sink
                .send(submission::W2IMessage::CompilationResult(program))
                .map_err(|e| {
                    errors::InvokerFailure(format!(
                        "Failed to send compilation result to sink: {:?}",
                        e
                    ))
                })?,
        };
    }

    Ok(())
}

impl Subprocess {
    // #[entrypoint("core_worker")]
    pub fn main(init: Vec<u8>) -> Result<(), errors::Error> {
        println!("Child main!");

        let init: InitializationInfo = rmp_serde::from_slice(&init).map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to parse initialization information as msgpack format: {:?}",
                e
            ))
        })?;

        let worker_space = sandbox::enter_worker_space(init.sandbox_config).map_err(|e| {
            errors::InvokerFailure(format!("Failed to enter worker space: {:?}", e))
        })?;

        let mut subprocess = Subprocess {
            rx_i2w_command: Mutex::new(init.rx_i2w_command),
            rx_i2w_urgent: Mutex::new(init.rx_i2w_urgent),
            tx_w2i: Mutex::new(init.tx_w2i),
            worker_space,
            current_command: Mutex::new(None),
            program: RwLock::new(None),
            language: init.language,
            source_files: init.source_files,
            dependency_dag: RwLock::new(init.dependency_dag),
        };

        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            errors::InvokerFailure(format!("Failed to create tokio runtime: {:?}", e))
        })?;
        rt.block_on(subprocess.work())
    }

    async fn work(&mut self) -> Result<(), errors::Error> {
        let commands_future = async {
            while let Some(command) = self.rx_i2w_command.lock().await.recv().await? {
                self.handle_core_command(command).await?;
            }
            Ok(()) as anyhow::Result<()>
        };

        let urgent_commands_future = async {
            while let Some(command) = self.rx_i2w_urgent.lock().await.recv().await? {
                self.handle_urgent_command(command).await?;
            }
            Ok(()) as anyhow::Result<()>
        };

        tokio::select! {
            _ = commands_future => {},
            _ = urgent_commands_future => {}
        };

        Ok(())
    }

    async fn handle_core_command(&self, command: submission::Command) -> Result<(), errors::Error> {
        let (handle, reg) = AbortHandle::new_pair();
        *self.current_command.lock().await = Some((command.clone(), handle));
        let ret = Abortable::new(self._handle_core_command(command), reg).await;
        *self.current_command.lock().await = None;
        ret.unwrap_or(Ok(())) // It's fine if the command was aborted
    }

    // Should only fail on critical operating failures (e.g. on communication failure); other errors
    // should be passed to the sink
    async fn _handle_core_command(
        &self,
        command: submission::Command,
    ) -> Result<(), errors::Error> {
        match command {
            submission::Command::Compile => {
                let result = self
                    .language
                    .build(
                        self.source_files.iter().map(|s| s.as_ref()).collect(),
                        &self.worker_space,
                    )
                    .await;
                if let Ok(ref program) = result {
                    *self.program.write().await = Some(program.clone());
                }
                self.tx_w2i
                    .lock()
                    .await
                    .send(W2INotification::CompilationResult(result))
                    .await
                    .map_err(|e| {
                        errors::InvokerFailure(format!(
                            "Failed to notify the worker after building a program: {:?}",
                            e
                        ))
                    })?;
            }

            submission::Command::Test(test) => {
                if !self.dependency_dag.read().await.is_test_enabled(test) {
                    return Ok(());
                }

                let lock = self.program.read().await;

                let program = lock.as_ref().ok_or_else(|| {
                    errors::InvokerFailure(
                        "Attempted to judge a program on a core before the core acquired a \
                         reference to the built program"
                            .to_string(),
                    )
                })?;

                // TODO: handle errors
                program.run(&self.worker_space).await?;
            }
        }

        Ok(())
    }

    async fn handle_urgent_command(&self, command: I2WUrgentCommand) -> Result<(), errors::Error> {
        match command {
            I2WUrgentCommand::AddFailedTests(tests) => {
                let mut dag = self.dependency_dag.write().await;
                for test in tests.into_iter() {
                    dag.fail_test(test);
                }
                let current_command = self.current_command.lock().await;
                if let Some((submission::Command::Test(test), ref handle)) = *current_command {
                    if !dag.is_test_enabled(test) {
                        handle.abort();
                    }
                }
            }
        }

        Ok(())
    }
}
