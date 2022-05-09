use crate::{
    errors,
    image::{language, program, sandbox},
    problem, submission,
};
use futures::future::{AbortHandle, Abortable};
use multiprocessing::tokio::{channel, Child, Receiver, Sender};
use multiprocessing::Object;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};

#[derive(Object)]
enum I2WUrgentCommand {
    AddFailedTests(Vec<u64>),
}

#[derive(Object)]
enum W2INotification {
    CompilationResult(Result<(program::Program, String), errors::Error>),
}

pub struct Worker {
    tx_i2w_command: Mutex<Sender<submission::Command>>,
    tx_i2w_urgent: Mutex<Sender<I2WUrgentCommand>>,
}

impl Worker {
    pub async fn new(
        language: language::Language,
        source_files: Vec<String>,
        sandbox_config: sandbox::SandboxConfig,
        dependency_dag: problem::dependencies::DependencyDAG,
        program: Option<program::Program>,
        event_sink: mpsc::UnboundedSender<submission::W2IMessage>,
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
                sandbox_config,
                dependency_dag,
                program,
            )
            .await
            .map_err(|e| {
                errors::InvokerFailure(format!("Failed to spawn a worker subprocess: {:?}", e))
            })?;

        let worker = Worker {
            tx_i2w_command: Mutex::new(tx_i2w_command),
            tx_i2w_urgent: Mutex::new(tx_i2w_urgent),
        };

        tokio::spawn(work_detached(rx_w2i, event_sink, child));

        Ok(worker)
    }

    pub async fn push_command(&self, command: submission::Command) -> Result<(), errors::Error> {
        self.tx_i2w_command
            .lock()
            .await
            .send(&command)
            .await
            .map_err(|e| {
                errors::InvokerFailure(format!("Failed to send command to the worker: {:?}", e))
            })
    }

    pub async fn add_failed_tests(&self, tests: Vec<u64>) -> Result<(), errors::Error> {
        self.tx_i2w_urgent
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
}

async fn work_detached(
    mut rx_w2i: Receiver<W2INotification>,
    event_sink: mpsc::UnboundedSender<submission::W2IMessage>,
    mut child: Child<Result<(), errors::Error>>,
) {
    let event_sink = &event_sink;
    let result = async move || -> Result<(), errors::Error> {
        while let Some(notification) = rx_w2i.recv().await.map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to read notification from worker subprocess: {:?}",
                e
            ))
        })? {
            match notification {
                W2INotification::CompilationResult(result) => event_sink
                    .send(submission::W2IMessage::CompilationResult(result))
                    .map_err(|e| {
                        errors::InvokerFailure(format!(
                            "Failed to send compilation result to sink: {:?}",
                            e
                        ))
                    })?,
            };
        }

        child
            .join()
            .await
            .map_err(|e| errors::InvokerFailure(format!("Worker process crashed: {:?}", e)))?
    }()
    .await;

    if let Err(err) = result {
        if let Err(send_err) = event_sink.send(submission::W2IMessage::Failure(err.clone())) {
            println!(
                "Failed to report error ({:?}) to event sink: {:?}",
                err, send_err
            );
        }
    }
}

struct Subprocess {
    worker_space: sandbox::WorkerSpace,
    current_command: Mutex<Option<(submission::Command, AbortHandle)>>,
    language: language::Language,
    source_files: Vec<String>,
    dependency_dag: RwLock<problem::dependencies::DependencyDAG>,
}

struct SubprocessMain {
    tx_w2i: Sender<W2INotification>,
    program: Option<program::Program>,
    rootfs_ns: Option<(sandbox::RootFS, sandbox::Namespace)>,
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
    tx_w2i: Sender<W2INotification>,
    language: language::Language,
    source_files: Vec<String>,
    sandbox_config: sandbox::SandboxConfig,
    dependency_dag: problem::dependencies::DependencyDAG,
    program: Option<program::Program>,
) -> Result<(), errors::Error> {
    let worker_space = sandbox::enter_worker_space(sandbox_config)
        .map_err(|e| errors::InvokerFailure(format!("Failed to enter worker space: {:?}", e)))?;

    let subprocess = Arc::new(Subprocess {
        worker_space,
        current_command: Mutex::new(None),
        language,
        source_files,
        dependency_dag: RwLock::new(dependency_dag),
    });

    let proc = subprocess.clone();
    let commands_future = tokio::spawn(async move {
        let mut main = SubprocessMain {
            tx_w2i,
            program,
            rootfs_ns: None,
        };

        while let Some(command) = rx_i2w_command.recv().await.map_err(|e| {
            errors::InvokerFailure(format!("Failed to receive command from invoker: {:?}", e))
        })? {
            proc.handle_core_command(command.clone(), &mut main).await?;
        }
        Ok(()) as Result<(), errors::Error>
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
        Ok(()) as Result<(), errors::Error>
    });

    commands_future.await.map_err(|e| {
        errors::InvokerFailure(format!("Worker main loop returned error: {:?}", e))
    })??;
    urgent_commands_future
        .await
        .map_err(|e| errors::InvokerFailure(format!("Worker urgent returned error: {:?}", e)))??;

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
        ret.unwrap_or(Ok(())) // It's fine if the command was aborted
    }

    // Should only fail on critical operating failures (e.g. on communication failure); other errors
    // should be passed to the sink
    async fn _handle_core_command(
        &self,
        command: submission::Command,
        main: &mut SubprocessMain,
    ) -> Result<(), errors::Error> {
        match command {
            submission::Command::Compile(build_id) => {
                let result = self
                    .language
                    .build(
                        self.source_files.iter().map(|s| s.as_ref()).collect(),
                        &self.worker_space,
                        build_id,
                    )
                    .await;
                if let Ok((ref program, _)) = result {
                    main.program = Some(program.clone());
                }
                main.tx_w2i
                    .send(&W2INotification::CompilationResult(result))
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

                let program = main.program.as_ref().ok_or_else(|| {
                    errors::InvokerFailure(
                        "Attempted to judge a program on a core before the core acquired a \
                         reference to the built program"
                            .to_string(),
                    )
                })?;

                if let None = main.rootfs_ns {
                    main.rootfs_ns = Some((
                        program.make_rootfs(&self.worker_space)?,
                        self.worker_space.make_namespace("run".to_string()).await?,
                    ));
                } else {
                    main.rootfs_ns.as_mut().unwrap().0.reset()?;
                }

                let (rootfs, ns) = main.rootfs_ns.as_mut().unwrap();
                program.run(rootfs, ns).await?;
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
