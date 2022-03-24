use crate::cgroups;
use anyhow::{bail, Context, Result};
use libc::{c_int, pid_t};
use signal_hook::{
    consts::{SIGCHLD, SIGKILL},
    iterator::{exfiltrator::origin::WithOrigin, SignalsInfo},
};
use std::collections::{HashMap, VecDeque};
use std::ops::{DerefMut, FnOnce};
use std::panic::UnwindSafe;
use std::sync::{Arc, Mutex};

pub struct CorePool<'a> {
    collector_thread: Option<std::thread::JoinHandle<()>>,
    info: Arc<Mutex<QueueInfo<'a>>>,
}

struct QueueInfo<'a> {
    cpuset_pool: Vec<cgroups::AffineCPUSet>,
    queued_tasks: VecDeque<Task<'a>>,
    pid_to_cpuset: HashMap<pid_t, cgroups::AffineCPUSet>,
    stop: bool,
    stopped: bool,
}

pub struct Task<'a> {
    pub callback: Box<dyn FnOnce() -> () + Sync + Send + UnwindSafe + 'a>,
    pub group: String,
}

impl<'a> CorePool<'a> {
    pub fn new(cores: Vec<u64>) -> Result<CorePool<'a>> {
        let mut cpusets: Vec<cgroups::AffineCPUSet> = Vec::new();
        for core in &cores {
            cpusets.push(
                cgroups::AffineCPUSet::new(*core)
                    .with_context(|| format!("Failed to create cpuset for core {}", core))?,
            );
        }

        let info = Arc::new(Mutex::new(QueueInfo {
            cpuset_pool: cpusets,
            queued_tasks: VecDeque::new(),
            pid_to_cpuset: HashMap::new(),
            stop: false,
            stopped: false,
        }));

        let mut signals = SignalsInfo::<WithOrigin>::new(&[SIGCHLD])?;

        let collector_thread;
        {
            let info = info.clone();
            let thread_fn: Box<dyn FnOnce() -> () + Send + 'a> = Box::new(move || {
                // This is a tight loop
                // This locks when no child processes are running anymore (i.e. the queue is empty)
                for _ in signals.forever() {
                    let mut info = info.lock().unwrap();
                    loop {
                        let mut wstatus: c_int = 0;
                        let pid;
                        unsafe {
                            // This locks when at least one process is still running, which is a very common situation.
                            // It would be more "correct" to use WNOHANG, but passing 0 allows us to wait for processes
                            // here, in waitpid, rather than in signals.forever(), which is a huge performance gain.
                            pid = libc::waitpid(-1, &mut wstatus as *mut c_int, 0);
                        }
                        if pid == -1 {
                            // TODO: ensure it's ECHILD and not something else
                            break;
                        }
                        if let Some(cpuset) = info.pid_to_cpuset.remove(&pid) {
                            // Start another task if the queue is not empty
                            if let Some(task) = info.queued_tasks.pop_front() {
                                // TODO: handle failures gracefully
                                let pid = Self::_spawn_in_cpuset(&cpuset, task.callback).unwrap();
                                info.pid_to_cpuset.insert(pid, cpuset);
                            } else {
                                info.cpuset_pool.push(cpuset);
                            }
                        }
                    }
                    if info.stop && info.pid_to_cpuset.is_empty() && info.queued_tasks.is_empty() {
                        info.stopped = true;
                        break;
                    }
                }
            });
            let thread_fn_static: Box<dyn FnOnce() -> () + Send + 'static> =
                unsafe { std::mem::transmute(thread_fn) };
            collector_thread = std::thread::spawn(thread_fn_static);
        }

        Ok(CorePool {
            collector_thread: Some(collector_thread),
            info,
        })
    }

    pub fn spawn_dedicated(&mut self, task: Task<'a>) -> Result<()> {
        let mut info = self
            .info
            .lock()
            .or_else(|_| bail!("CorePool lock is poisoned"))?;
        if info.cpuset_pool.is_empty() {
            info.queued_tasks.push_back(task);
        } else {
            let cpuset = info.cpuset_pool.last().unwrap();
            let pid = Self::_spawn_in_cpuset(cpuset, task.callback)?;
            let cpuset = info.cpuset_pool.pop().unwrap(); // only pop on success
            info.pid_to_cpuset.insert(pid, cpuset);
        }
        Ok(())
    }

    fn _spawn_in_cpuset<F: FnOnce() -> ()>(cpuset: &cgroups::AffineCPUSet, f: F) -> Result<pid_t>
    where
        F: Sync + Send + UnwindSafe + 'a,
    {
        let child_pid = unsafe { libc::fork() };
        if child_pid == -1 {
            bail!("fork() failed");
        } else if child_pid == 0 {
            let panic = std::panic::catch_unwind(|| {
                cpuset.add_task(unsafe { libc::getpid() }).unwrap();
                f();
            });
            let exit_code = if panic.is_ok() { 0 } else { 1 };
            unsafe {
                libc::exit(exit_code);
            }
        } else {
            Ok(child_pid)
        }
    }

    fn _join(&mut self) {
        {
            let mut info = self.info.lock().unwrap();
            if info.stopped {
                return;
            }
            info.stop = true;
        }
        // Wake up thread and tell it to stop when everything is joined
        unsafe {
            libc::raise(libc::SIGCHLD);
        }
        // Join the thread
        self.collector_thread
            .take()
            .unwrap()
            .join()
            .expect("Failed to join collector thread");
    }

    pub fn join(mut self) {
        self._join();
    }
}

impl Drop for CorePool<'_> {
    fn drop(&mut self) {
        {
            let mut info = self.info.lock().unwrap();
            let info = info.deref_mut();
            // Unqueue everything and terminate running processes immediately
            info.queued_tasks.clear();
            for (pid, cpuset) in info.pid_to_cpuset.drain() {
                unsafe {
                    libc::kill(pid, SIGKILL);
                }
                info.cpuset_pool.push(cpuset);
            }
        }
        self._join();
    }
}
