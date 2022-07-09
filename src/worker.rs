use crate::{
    errors,
    errors::{ToError, ToResult},
    image::{language, program, sandbox, strategy},
    problem::{problem, verdict},
    submission,
};
use futures::{
    future::{AbortHandle, Abortable},
    StreamExt,
};
use multiprocessing::tokio::{channel, Child, Receiver, Sender};
use multiprocessing::Object;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};

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
    child: Arc<Mutex<(Child<Result<(), errors::Error>>, Option<errors::Error>)>>,
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
        invocation_limits: HashMap<String, verdict::InvocationLimit>,
    ) -> Result<Worker, errors::Error> {
        let (tx_i2w_command, rx_i2w_command) =
            channel().context_invoker("Failed to create an IPC channel")?;
        let (tx_i2w_urgent, rx_i2w_urgent) =
            channel().context_invoker("Failed to create an IPC channel")?;
        let (tx_w2i, rx_w2i) = channel().context_invoker("Failed to create an IPC channel")?;

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
                invocation_limits,
            )
            .await
            .context_invoker("Failed to spawn a worker subprocess")?;

        Ok(Worker {
            tx_i2w_command: Some(Arc::new(Mutex::new(tx_i2w_command))),
            tx_i2w_urgent: Some(Mutex::new(tx_i2w_urgent)),
            rx_w2i: Arc::new(Mutex::new(rx_w2i)),
            child: Arc::new(Mutex::new((child, None))),
        })
    }

    pub async fn execute_command(
        &self,
        command: submission::Command,
        n_messages: usize,
    ) -> Result<impl futures::stream::Stream<Item = W2IMessage>, errors::Error> {
        let tx_i2w_command = self
            .tx_i2w_command
            .as_ref()
            .context_invoker("Cannot execute command on worker after finalization")?
            .clone();

        let rx_w2i = self.rx_w2i.clone();
        let child = self.child.clone();

        let (tx, rx) = mpsc::unbounded_channel();

        // After we send the value to tx, the coroutine is no longer abortable
        tokio::spawn(async move {
            let res: Result<(), errors::Error> = try {
                let mut rx_w2i = rx_w2i.lock().await;

                tx_i2w_command
                    .lock()
                    .await
                    .send(&command)
                    .await
                    .context_invoker("Failed to send command to the worker")?;

                for _ in 0..n_messages {
                    let msg = rx_w2i
                        .recv()
                        .await
                        .context_invoker("Failed to receive response from the worker")?;

                    let msg = match msg {
                        Some(msg) => msg,
                        None => {
                            // The worker must have terminated preemptively; this shouldn't happen
                            let mut child = child.lock().await;

                            if child.1.is_none() {
                                child.1 = Some(
                                    child
                                        .0
                                        .join()
                                        .await
                                        .context_invoker("Failed to join child")?
                                        .err()
                                        .unwrap_or_else(|| {
                                            errors::InvokerFailure(
                                                "(no error reported)".to_string(),
                                            )
                                        }),
                                );
                            }

                            W2IMessage::Failure(
                                child
                                    .1
                                    .as_ref()
                                    .unwrap()
                                    .context_invoker("Worker terminated preemptively"),
                            )
                        }
                    };

                    if let Err(e) = tx.send(msg) {
                        println!("Response to a command is ignored: {:?}", e);
                    }
                }
            };

            if let Err(e) = res {
                println!("Error while executing a worker command: {:?}", e);
            }
        });

        Ok(tokio_stream::wrappers::UnboundedReceiverStream::new(rx))
    }

    pub async fn add_failed_tests(&self, tests: Vec<u64>) -> Result<(), errors::Error> {
        self.tx_i2w_urgent
            .as_ref()
            .context_invoker("Cannot add failed tests after finalization")?
            .lock()
            .await
            .send(&I2WUrgentCommand::AddFailedTests(tests))
            .await
            .context_invoker("Failed to notify the worker subprocess about failed tests")
    }

    pub async fn finalize(&mut self) -> Result<(), errors::Error> {
        self.tx_i2w_command = None;
        self.tx_i2w_urgent = None;
        let response = self
            .execute_command(submission::Command::Finalize, 1)
            .await?
            .next()
            .await;
        match response {
            Some(W2IMessage::Finalized) => Ok(()),
            Some(W2IMessage::Failure(e)) => Err(e),
            _ => Err(errors::InvokerFailure(format!(
                "Unexpected response to finalization request: {response:?}"
            ))),
        }
    }
}

struct Subprocess {
    current_test: Mutex<Option<(u64, AbortHandle)>>,
    language: language::Language,
    source_files: Vec<String>,
    instantiated_dependency_dag: RwLock<problem::InstantiatedDependencyDAG>,
    core: u64,
}

struct SubprocessMain {
    tx_w2i: Sender<W2IMessage>,
    strategy_factory: strategy::StrategyFactory,
    strategy: Option<strategy::Strategy>,
    problem_revision_data: problem::ProblemRevisionData,
    invocation_limits: Option<HashMap<String, verdict::InvocationLimit>>,
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
    invocation_limits: HashMap<String, verdict::InvocationLimit>,
) -> Result<(), errors::Error> {
    let mut tx_w2i = {
        sandbox::enter_worker_space(core).context_invoker("Failed to enter worker space")?;

        let mut invocation_limits = Some(invocation_limits);

        let strategy = match program {
            Some(ref program) => Some(
                strategy_factory
                    .make(program, invocation_limits.take().unwrap(), core)
                    .await?,
            ),
            None => None,
        };

        let subprocess = Arc::new(Subprocess {
            current_test: Mutex::new(None),
            language,
            source_files,
            instantiated_dependency_dag: RwLock::new(instantiated_dependency_dag),
            core,
        });

        let proc = subprocess.clone();
        let commands_future = tokio::spawn(async move {
            let mut main = SubprocessMain {
                tx_w2i,
                strategy_factory,
                strategy,
                problem_revision_data,
                invocation_limits,
            };

            while let Some(command) = rx_i2w_command
                .recv()
                .await
                .context_invoker("Failed to receive command from invoker")?
            {
                proc.handle_core_command(command.clone(), &mut main).await?;
            }

            Ok(main.tx_w2i)
        });

        let urgent_commands_future = tokio::spawn(async move {
            while let Some(command) = rx_i2w_urgent
                .recv()
                .await
                .context_invoker("Failed to receive urgent command from invoker")?
            {
                subprocess.handle_urgent_command(command).await?;
            }
            Ok(())
        });

        let tx_w2i = commands_future
            .await
            .context_invoker("Worker main loop returned error")??;

        urgent_commands_future
            .await
            .context_invoker("Worker urgent loop returned error")??;

        tx_w2i
    };

    tx_w2i
        .send(&W2IMessage::Finalized)
        .await
        .context_invoker("Failed to send finalization notification to invoker")?;

    Ok(())
}

impl Subprocess {
    async fn handle_core_command(
        &self,
        command: submission::Command,
        main: &mut SubprocessMain,
    ) -> Result<(), errors::Error> {
        match command {
            submission::Command::Compile(build_id) => {
                let res: Result<W2IMessage, errors::Error> = try {
                    let (program, log) = self
                        .language
                        .build(
                            self.source_files.iter().map(|s| s.as_ref()).collect(),
                            build_id,
                        )
                        .await?;
                    main.strategy = Some(
                        main.strategy_factory
                            .make(&program, main.invocation_limits.take().unwrap(), self.core)
                            .await?,
                    );
                    W2IMessage::CompilationResult(program, log)
                };
                let res = res.unwrap_or_else(|e| W2IMessage::Failure(e));
                main.tx_w2i
                    .send(&res)
                    .await
                    .context_invoker("Failed to send command result to invoker")
            }

            submission::Command::Test(tests) => {
                let strategy = main.strategy.as_mut().context_invoker(
                    "Attempted to judge a program on a core before the core acquired a reference \
                     to the built program",
                )?;

                for test in tests {
                    if !self
                        .instantiated_dependency_dag
                        .read()
                        .await
                        .is_test_enabled(test)
                    {
                        main.tx_w2i
                            .send(&W2IMessage::Aborted)
                            .await
                            .context_invoker("Failed to send command result to invoker")?;
                        continue;
                    }

                    let (handle, reg) = AbortHandle::new_pair();
                    *self.current_test.lock().await = Some((test, handle));

                    let result = Abortable::new(
                        async {
                            match strategy
                                .invoke(
                                    "run".to_string(),
                                    main.strategy_factory
                                        .root
                                        .join("tests")
                                        .join(test.to_string()),
                                )
                                .await
                            {
                                Ok(result) => W2IMessage::TestResult(result),
                                Err(e) => W2IMessage::Failure(e),
                            }
                        },
                        reg,
                    )
                    .await;

                    *self.current_test.lock().await = None;

                    let message = result.unwrap_or(W2IMessage::Aborted);

                    main.tx_w2i
                        .send(&message)
                        .await
                        .context_invoker("Failed to send command result to invoker")?;
                }

                Ok(())
            }

            submission::Command::Finalize => main
                .tx_w2i
                .send(&W2IMessage::Finalized)
                .await
                .context_invoker("Failed to send command result to invoker"),
        }
    }

    async fn handle_urgent_command(&self, command: I2WUrgentCommand) -> Result<(), errors::Error> {
        match command {
            I2WUrgentCommand::AddFailedTests(tests) => {
                let mut dag = self.instantiated_dependency_dag.write().await;
                for test in tests.into_iter() {
                    dag.fail_test(test);
                }
                let current_test = self.current_test.lock().await;
                if let Some((test, ref handle)) = *current_test {
                    if !dag.is_test_enabled(test) {
                        handle.abort();
                    }
                }
            }
        }

        Ok(())
    }
}
