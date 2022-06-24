use crate::{
    errors,
    image::{language, program, sandbox, strategy},
    problem::{problem, verdict},
    submission,
};
use futures::future::{AbortHandle, Abortable};
use multiprocessing::tokio::{channel, Child, Receiver, Sender};
use multiprocessing::Object;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

#[derive(Object)]
enum I2WUrgentCommand {
    AddFailedTests(Vec<u64>),
}

#[derive(Debug, Object)]
pub enum W2IMessage {
    CompilationResult(program::Program, String),
    TestResult(verdict::TestJudgementResult),
    Finalized,
    Failure(errors::Error),
    Aborted,
}

pub struct Worker {
    tx_i2w_command: Option<Arc<Mutex<Sender<submission::Command>>>>,
    tx_i2w_urgent: Option<Mutex<Sender<I2WUrgentCommand>>>,
    rx_w2i: Arc<Mutex<Receiver<W2IMessage>>>,
    child: Child<Result<(), errors::Error>>,
}

impl Worker {
    pub async fn new(
        language: language::Language,
        source_files: Vec<String>,
        core: u64,
        instantiated_dependency_dag: problem::InstantiatedDependencyDAG,
        program: Option<program::Program>,
        strategy_factory: strategy::StrategyFactory,
        problem_revision_data: problem::ProblemRevisionData,
    ) -> Result<Worker, errors::Error> {
        let (tx_i2w_command, rx_i2w_command) = channel().map_err(|e| {
            errors::InvokerFailure(format!("Failed to create an IPC channel: {:?}", e))
        })?;
        let (tx_i2w_urgent, rx_i2w_urgent) = channel().map_err(|e| {
            errors::InvokerFailure(format!("Failed to create an IPC channel: {:?}", e))
        })?;
        let (tx_w2i, rx_w2i) = channel().map_err(|e| {
            errors::InvokerFailure(format!("Failed to create an IPC channel: {:?}", e))
        })?;

        let child = subprocess_main
            .spawn_tokio(
                rx_i2w_command,
                rx_i2w_urgent,
                tx_w2i,
                language,
                source_files,
                core,
                instantiated_dependency_dag,
                program,
                strategy_factory,
                problem_revision_data,
            )
            .await
            .map_err(|e| {
                errors::InvokerFailure(format!("Failed to spawn a worker subprocess: {:?}", e))
            })?;

        Ok(Worker {
            tx_i2w_command: Some(Arc::new(Mutex::new(tx_i2w_command))),
            tx_i2w_urgent: Some(Mutex::new(tx_i2w_urgent)),
            rx_w2i: Arc::new(Mutex::new(rx_w2i)),
            child,
        })
    }

    pub async fn execute_command(
        &self,
        command: submission::Command,
    ) -> Result<W2IMessage, errors::Error> {
        // The order is:
        // 1. Lock tx
        // 2. Send command
        // 3. Start locking rx
        // 4. Release tx
        // 5. Lock rx
        // 6. Receive reply
        // 7. Release rx
        // This way no two messages are sent or received at the same time, and the order in which
        // tasks receive replies is the same that commands were sent in, because tokio's Mutex is
        // FIFO.
        let tx_i2w_command = self
            .tx_i2w_command
            .as_ref()
            .ok_or_else(|| {
                errors::InvokerFailure(
                    "Cannot execute command on worker after finalization".to_string(),
                )
            })?
            .clone();
        let rx_w2i = self.rx_w2i.clone();
        // After we send the value to tx, the coroutine is no longer abortable
        tokio::spawn(async move {
            let mut tx_i2w_command = tx_i2w_command.lock().await;
            tx_i2w_command.send(&command).await.map_err(|e| {
                errors::InvokerFailure(format!("Failed to send command to the worker: {:?}", e))
            })?;
            let rx_w2i = rx_w2i.lock(); // TODO: does this actually start locking?
            drop(tx_i2w_command);
            let mut rx_w2i = rx_w2i.await;
            rx_w2i
                .recv()
                .await
                .map_err(|e| {
                    errors::InvokerFailure(format!(
                        "Failed to receive response to {:?} from the worker: {:?}",
                        command, e
                    ))
                })?
                .ok_or_else(|| {
                    errors::InvokerFailure(format!("No response to {:?} from the worker", command))
                })
        })
        .await
        .map_err(|e| errors::InvokerFailure(format!("Panic in tokio task: {:?}", e)))?
    }

    pub async fn add_failed_tests(&self, tests: Vec<u64>) -> Result<(), errors::Error> {
        self.tx_i2w_urgent
            .as_ref()
            .ok_or_else(|| {
                errors::InvokerFailure("Cannot add failed tests after finalization".to_string())
            })?
            .lock()
            .await
            .send(&I2WUrgentCommand::AddFailedTests(tests))
            .await
            .map_err(|e| {
                errors::InvokerFailure(format!(
                    "Failed to notify the worker subprocess about failed tests: {:?}",
                    e
                ))
            })
    }

    pub async fn finalize(&mut self) -> Result<(), errors::Error> {
        self.tx_i2w_command = None;
        self.tx_i2w_urgent = None;
        let response = self.execute_command(submission::Command::Finalize).await?;
        match response {
            W2IMessage::Finalized => Ok(()),
            W2IMessage::Failure(e) => Err(e),
            _ => Err(errors::InvokerFailure(format!(
                "Unexpected response to finalization request: {:?}",
                response
            ))),
        }
    }
}

struct Subprocess {
    current_command: Mutex<Option<(submission::Command, AbortHandle)>>,
    language: language::Language,
    source_files: Vec<String>,
    instantiated_dependency_dag: RwLock<problem::InstantiatedDependencyDAG>,
}

struct SubprocessMain {
    tx_w2i: Sender<W2IMessage>,
    strategy_factory: strategy::StrategyFactory,
    strategy: Option<strategy::Strategy>,
    problem_revision_data: problem::ProblemRevisionData,
}

// multithreading does not interact with sandboxing well. For one thing, unshare only seems to apply
// to the current thread rather than the whole process. /proc/self/mounts refers to the mount
// namespace of the main thread of the process, and different threads of the same process can be in
// different mount namespaces despite what man says, which leads to sandbox escape. Disabling
// multithreading seems like the most robust solution.
#[multiprocessing::entrypoint]
#[tokio::main(flavor = "current_thread")]
pub async fn subprocess_main(
    mut rx_i2w_command: Receiver<submission::Command>,
    mut rx_i2w_urgent: Receiver<I2WUrgentCommand>,
    tx_w2i: Sender<W2IMessage>,
    language: language::Language,
    source_files: Vec<String>,
    core: u64,
    instantiated_dependency_dag: problem::InstantiatedDependencyDAG,
    program: Option<program::Program>,
    strategy_factory: strategy::StrategyFactory,
    problem_revision_data: problem::ProblemRevisionData,
) -> Result<(), errors::Error> {
    let mut tx_w2i = {
        sandbox::enter_worker_space(core).map_err(|e| {
            errors::InvokerFailure(format!("Failed to enter worker space: {:?}", e))
        })?;

        let strategy = match program {
            Some(ref program) => Some(strategy_factory.make(program).await?),
            None => None,
        };

        let subprocess = Arc::new(Subprocess {
            current_command: Mutex::new(None),
            language,
            source_files,
            instantiated_dependency_dag: RwLock::new(instantiated_dependency_dag),
        });

        let proc = subprocess.clone();
        let commands_future = tokio::spawn(async move {
            let mut main = SubprocessMain {
                tx_w2i,
                strategy_factory,
                strategy,
                problem_revision_data,
            };

            while let Some(command) = rx_i2w_command.recv().await.map_err(|e| {
                errors::InvokerFailure(format!("Failed to receive command from invoker: {:?}", e))
            })? {
                proc.handle_core_command(command.clone(), &mut main).await?;
            }

            Ok(main.tx_w2i)
        });

        let urgent_commands_future = tokio::spawn(async move {
            while let Some(command) = rx_i2w_urgent.recv().await.map_err(|e| {
                errors::InvokerFailure(format!(
                    "Failed to receive urgent command from invoker: {:?}",
                    e
                ))
            })? {
                subprocess.handle_urgent_command(command).await?;
            }
            Ok(())
        });

        let tx_w2i = commands_future.await.map_err(|e| {
            errors::InvokerFailure(format!("Worker main loop returned error: {:?}", e))
        })??;

        urgent_commands_future.await.map_err(|e| {
            errors::InvokerFailure(format!("Worker urgent returned error: {:?}", e))
        })??;

        tx_w2i
    };

    tx_w2i.send(&W2IMessage::Finalized).await.map_err(|e| {
        errors::InvokerFailure(format!(
            "Failed to send finalization notification to invoker: {:?}",
            e
        ))
    })?;

    Ok(())
}

impl Subprocess {
    async fn handle_core_command(
        &self,
        command: submission::Command,
        main: &mut SubprocessMain,
    ) -> Result<(), errors::Error> {
        let (handle, reg) = AbortHandle::new_pair();
        *self.current_command.lock().await = Some((command.clone(), handle));
        let ret = Abortable::new(self._handle_core_command(command, main), reg).await;
        *self.current_command.lock().await = None;
        let message = ret
            .unwrap_or(Ok(W2IMessage::Aborted))
            .unwrap_or_else(|e| W2IMessage::Failure(e));
        if let W2IMessage::Finalized = message {
            Ok(())
        } else {
            main.tx_w2i.send(&message).await.map_err(|e| {
                errors::InvokerFailure(format!("Failed to send command result to invoker: {:?}", e))
            })
        }
    }

    async fn _handle_core_command(
        &self,
        command: submission::Command,
        main: &mut SubprocessMain,
    ) -> Result<W2IMessage, errors::Error> {
        match command {
            submission::Command::Compile(build_id) => {
                let (program, log) = self
                    .language
                    .build(
                        self.source_files.iter().map(|s| s.as_ref()).collect(),
                        build_id,
                    )
                    .await?;
                main.strategy = Some(main.strategy_factory.make(&program).await?);
                Ok(W2IMessage::CompilationResult(program, log))
            }

            submission::Command::Test(test) => {
                if !self
                    .instantiated_dependency_dag
                    .read()
                    .await
                    .is_test_enabled(test)
                {
                    return Ok(W2IMessage::Aborted);
                }

                let strategy = main.strategy.as_mut().ok_or_else(|| {
                    errors::InvokerFailure(
                        "Attempted to judge a program on a core before the core acquired a \
                         reference to the built program"
                            .to_string(),
                    )
                })?;

                let result = strategy
                    .invoke(
                        "run".to_string(),
                        main.strategy_factory
                            .root
                            .join("tests")
                            .join(test.to_string()),
                    )
                    .await?;
                Ok(W2IMessage::TestResult(result))
            }

            submission::Command::Finalize => Ok(W2IMessage::Finalized),
        }
    }

    async fn handle_urgent_command(&self, command: I2WUrgentCommand) -> Result<(), errors::Error> {
        match command {
            I2WUrgentCommand::AddFailedTests(tests) => {
                let mut dag = self.instantiated_dependency_dag.write().await;
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
