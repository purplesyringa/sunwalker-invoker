use crate::{
    image::{language, sandbox},
    problem,
};
use anyhow::{anyhow, Context, Result};
use futures::{
    future::{AbortHandle, Abortable},
    pin_mut,
};
use std::collections::HashMap;
use std::sync::{atomic, Arc, Weak};
use tokio::select;
use tokio::sync::{mpsc, Mutex, RwLock};

#[derive(Clone)]
pub enum CoreCommand {
    Compile,
    Test(u64),
}

#[derive(Debug)]
pub enum UrgentCommand {
    AbortCurrentCommandIfNecessary,
    AbortAll,
}

pub struct CoreWorker {
    pub core: u64,
    rx: Mutex<mpsc::UnboundedReceiver<CoreCommand>>,
    pub tx: mpsc::UnboundedSender<CoreCommand>,
    rx_urgent: Mutex<mpsc::UnboundedReceiver<UrgentCommand>>,
    tx_urgent: mpsc::UnboundedSender<UrgentCommand>,
    pub current_command: Mutex<Option<CoreCommand>>,
    pub submission_info: Arc<SubmissionInfo>,
    pub worker_space: sandbox::WorkerSpace,
    pub done: atomic::AtomicBool,
}

pub struct SubmissionInfo {
    pub id: String,
    pub dependency_dag: RwLock<problem::dependencies::DependencyDAG>,
    pub language: language::Language,
    pub source_files: Vec<String>,
    pub built_program: RwLock<Option<language::Program>>,
    pub workers: Mutex<HashMap<u64, Weak<CoreWorker>>>,
}

impl CoreWorker {
    pub fn new(
        core: u64,
        submission_info: Arc<SubmissionInfo>,
        sandbox_config: sandbox::SandboxConfig,
    ) -> Result<CoreWorker> {
        let (tx, rx): (
            mpsc::UnboundedSender<CoreCommand>,
            mpsc::UnboundedReceiver<CoreCommand>,
        ) = mpsc::unbounded_channel();

        let (tx_urgent, rx_urgent): (
            mpsc::UnboundedSender<UrgentCommand>,
            mpsc::UnboundedReceiver<UrgentCommand>,
        ) = mpsc::unbounded_channel();

        let worker_space = sandbox::enter_worker_space(sandbox_config)
            .with_context(|| format!("Failed to enter worker space"))?;

        Ok(CoreWorker {
            core,
            rx: Mutex::new(rx),
            tx,
            rx_urgent: Mutex::new(rx_urgent),
            tx_urgent,
            current_command: Mutex::new(None),
            submission_info,
            worker_space,
            done: atomic::AtomicBool::new(false),
        })
    }

    pub async fn work(&self) -> Result<()> {
        // Commands from tx/rx are handled in FIFO order sequentially. Urgent commands arriving on
        // tx_urgent from rx_urgent are handled in FIFO order sequentially too, but take precedence
        // over normal commands and start execution without waiting for the last normal command to
        // finish (but not vice versa).
        let mut rx = self.rx.lock().await;
        let mut rx_urgent = self.rx_urgent.lock().await;
        while !self.done.load(atomic::Ordering::Relaxed) {
            select! {
                command = rx.recv() => {
                    match command {
                        None => break,
                        Some(command) => {
                            let (abort_handle, abort_registration) = AbortHandle::new_pair();
                            let future = Abortable::new(self.handle_core_command(command), abort_registration);
                            pin_mut!(future);
                            loop {
                                select! {
                                    res = &mut future => {
                                        println!("Returned {:?}", res);
                                        break;
                                    }
                                    urgent_command = rx_urgent.recv() => {
                                        match urgent_command {
                                            None => unimplemented!(),  // not sure how this should be handled
                                            Some(command) => {
                                                if self.handle_urgent_command(command).await? {
                                                    abort_handle.abort();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                urgent_command = rx_urgent.recv() => {
                    match urgent_command {
                        None => unimplemented!(),  // not sure how this should be handled
                        Some(command) => {
                            self.handle_urgent_command(command).await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_core_command(&self, command: CoreCommand) -> Result<()> {
        *self.current_command.lock().await = Some(command.clone());

        match command {
            CoreCommand::Compile => {
                let program = {
                    self.submission_info
                        .language
                        .build(
                            self.submission_info
                                .source_files
                                .iter()
                                .map(|s| s.as_ref())
                                .collect(),
                            &self.worker_space,
                        )
                        .await
                        .with_context(|| {
                            format!("Failed to build submission {}", self.submission_info.id)
                        })?
                };
                *self.submission_info.built_program.write().await = Some(program);
            }
            CoreCommand::Test(test) => {
                if !self
                    .submission_info
                    .dependency_dag
                    .read()
                    .await
                    .is_test_enabled(test)
                {
                    return Ok(());
                }

                let lock = self.submission_info.built_program.read().await;

                let program = lock
                    .as_ref()
                    .ok_or_else(|| anyhow!("Cannot run a program before it is compiled"))?;

                self.submission_info
                    .language
                    .run(&self.worker_space, program)
                    .await
                    .with_context(|| {
                        format!("Failed to run submission {}", self.submission_info.id)
                    })?;
            }
        }

        *self.current_command.lock().await = None;
        Ok(())
    }

    async fn handle_urgent_command(&self, command: UrgentCommand) -> Result<bool> {
        match command {
            UrgentCommand::AbortCurrentCommandIfNecessary => {
                let current_command = self.current_command.lock().await;
                match *current_command {
                    Some(CoreCommand::Test(test)) => {
                        if !self
                            .submission_info
                            .dependency_dag
                            .read()
                            .await
                            .is_test_enabled(test)
                        {
                            return Ok(true);
                        }
                    }
                    _ => (),
                }
            }
            UrgentCommand::AbortAll => {
                self.done.store(true, atomic::Ordering::Relaxed);
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn push(&self, command: CoreCommand) -> Result<()> {
        self.tx
            .send(command)
            .map_err(|_| anyhow!("Failed to send command to core"))
    }

    pub async fn abort_current_command_if_necessary(&self) -> Result<()> {
        let current_command = self.current_command.lock().await;
        match *current_command {
            Some(CoreCommand::Test(test)) => {
                if !self
                    .submission_info
                    .dependency_dag
                    .read()
                    .await
                    .is_test_enabled(test)
                {
                    self.tx_urgent
                        .send(UrgentCommand::AbortCurrentCommandIfNecessary)?;
                }
            }
            _ => (),
        }
        Ok(())
    }

    pub fn abort_all(&self) -> Result<()> {
        self.tx_urgent.send(UrgentCommand::AbortAll)?;
        Ok(())
    }
}
