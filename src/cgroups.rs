use anyhow::{bail, Context, Result};
use libc::pid_t;
use std::collections::HashSet;
use std::io::{BufRead, Write};

const CPUSET_ROOT: &str = "/sys/fs/cgroup/cpuset";

pub fn create_root_cpuset() -> Result<()> {
    std::fs::create_dir(format!("{}/sunwalker_root", CPUSET_ROOT))
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })
        .with_context(|| format!("Unable to create {}/sunwalker_root directory", CPUSET_ROOT))?;

    // Inherit mems
    std::fs::write(
        format!("{}/sunwalker_root/cpuset.mems", CPUSET_ROOT),
        std::fs::read_to_string(format!("{}/cpuset.mems", CPUSET_ROOT))?,
    )
    .with_context(|| {
        format!(
            "Failed to write to {}/sunwalker_root/cpuset.mems",
            CPUSET_ROOT
        )
    })?;

    // Move all tasks that don't yet belong to a cpuset to the root cpuset
    // FIXME: this is inherently racy, what can we do to avoid the race condition?
    let mut tids: Vec<pid_t> = Vec::new();

    for proc_entry in std::fs::read_dir("/proc")? {
        let proc_entry = proc_entry?;
        let proc_name = proc_entry.file_name();

        if let Ok(proc_name) = proc_name.into_string() {
            if let Ok(_) = proc_name.parse::<pid_t>() {
                let mut tasks_path = proc_entry.path();
                tasks_path.push("task");

                // Permission error? Race condition?
                if let Ok(tasks_it) = std::fs::read_dir(tasks_path) {
                    for task_entry in tasks_it {
                        let task_entry = task_entry?;
                        let tid = task_entry
                            .file_name()
                            .into_string()
                            .or_else(|e| bail!("Unable to parse task ID as string: {:?}", e))?
                            .parse::<pid_t>()?;

                        let mut cpuset_path = task_entry.path();
                        cpuset_path.push("cpuset");

                        // Idem
                        if let Ok(cpuset) = std::fs::read_to_string(cpuset_path) {
                            if cpuset == "/\n" {
                                // Does not belong to any cpuset yet
                                tids.push(tid);
                            } else {
                                // TODO: issue a warning or something
                            }
                        }
                    }
                }
            }
        }
    }

    let mut tasks_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(format!("{}/sunwalker_root/tasks", CPUSET_ROOT))
        .with_context(|| {
            format!(
                "Cannot open {}/sunwalker_root/tasks for writing",
                CPUSET_ROOT
            )
        })?;

    for tid in tids {
        // A failure most likely indicates a kernel thread, which cannot be rescheduled
        let _ = tasks_file.write_all(tid.to_string().as_ref());
    }

    Ok(())
}

#[derive(Debug)]
pub struct AffineCPUSet {
    core: u64,
}

impl AffineCPUSet {
    pub fn new(core: u64) -> Result<AffineCPUSet> {
        let dir = format!("{}/sunwalker_cpu_{}", CPUSET_ROOT, core);

        std::fs::create_dir(&dir)
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    Ok(())
                } else {
                    Err(e)
                }
            })
            .with_context(|| format!("Unable to create {} directory", dir))?;

        std::fs::write(format!("{}/cpuset.cpus", dir), core.to_string())
            .with_context(|| format!("Failed to write to {}/cpuset.cpus", dir))?;

        std::fs::write(
            format!("{}/cpuset.mems", dir),
            std::fs::read_to_string(format!("{}/cpuset.mems", CPUSET_ROOT))?,
        )
        .with_context(|| format!("Failed to write to {}/cpuset.mems", dir))?;

        if let Err(e) =
            std::fs::write(format!("{}/cpuset.cpu_exclusive", dir), "1").with_context(|| {
                format!(
                    "Failed to write to {}/cpuset.cpu_exclusive (is core {} in use?)",
                    dir, core
                )
            })
        {
            println!("[!] Failed to acquire exclusive access to core {} from the kernel. This usually indicates the presence of tasks with explicit CPU affinity. Make sure you do not have any such services running, or their list of CPU cores is limited. Docker is a common cause of the problem.\n{:?}", core, e);
        }

        Ok(AffineCPUSet { core })
    }

    pub fn add_task(&self, tid: pid_t) -> Result<()> {
        let path = format!("{}/sunwalker_cpu_{}/tasks", CPUSET_ROOT, self.core);
        std::fs::write(path, tid.to_string()).with_context(|| {
            format!(
                "Could not set affinity of task {} to CPU {}",
                tid, self.core
            )
        })
    }
}

pub fn drop_existing_affine_cpusets() -> Result<()> {
    let mut root_tasks_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(format!("{}/tasks", CPUSET_ROOT))
        .with_context(|| format!("Cannot open {}/tasks for writing", CPUSET_ROOT))?;

    for entry in
        std::fs::read_dir(CPUSET_ROOT).with_context(|| format!("Cannot read {}", CPUSET_ROOT))?
    {
        let entry = entry?;
        if let Ok(cpuset_name) = entry.file_name().into_string() {
            if cpuset_name.starts_with("sunwalker_cpu_") {
                // Move all tasks out from /sunwalker_cpu_*
                // FIXME: we should also send them SIGKILL, but this has terrible consequences in case of race condition
                let mut tasks_path = entry.path();
                tasks_path.push("tasks");
                let file = std::fs::File::open(&tasks_path)
                    .with_context(|| format!("Cannot open {:?} for reading", &tasks_path))?;
                for line in std::io::BufReader::new(file).lines() {
                    let tid: pid_t = line?
                        .parse()
                        .with_context(|| format!("Invalid TID in {:?}", tasks_path))?;
                    root_tasks_file.write_all(tid.to_string().as_ref())?;
                }

                // Remove cpuset
                std::fs::remove_dir(entry.path())?;
            }
        }
    }

    Ok(())
}

fn parse_cpuset_list(s: &str) -> Result<Vec<u64>> {
    let mut result: Vec<u64> = Vec::new();
    for part in s.trim().split(',') {
        if part.contains('-') {
            let bounds: Vec<&str> = part.split('-').collect();
            if bounds.len() != 2 {
                bail!("Invalid cpuset: {}", part);
            }
            let first = bounds[0]
                .parse()
                .with_context(|| format!("Invalid cpuset: {}", part))?;
            let last = bounds[1]
                .parse()
                .with_context(|| format!("Invalid cpuset: {}", part))?;
            for item in first..=last {
                result.push(item);
            }
        } else {
            result.push(
                part.parse()
                    .with_context(|| format!("Invalid cpuset: {}", part))?,
            );
        }
    }
    Ok(result)
}

fn format_cpuset_list(mut list: Vec<u64>) -> String {
    list.sort();
    list.iter()
        .map(|x| x.to_string())
        .collect::<Vec<String>>()
        .join(",")
}

fn get_all_cores() -> Result<Vec<u64>> {
    let path = format!("{}/cpuset.cpus", CPUSET_ROOT);
    let all_cores = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read a list of CPU cores from {}", path))?;
    parse_cpuset_list(all_cores.as_ref())
}

pub fn isolate_cores(isolated_cores: &Vec<u64>) -> Result<()> {
    let all_cores = get_all_cores()?;

    let mut not_isolated_cores: HashSet<u64> = HashSet::from_iter(all_cores.iter().cloned());
    for core in isolated_cores {
        if !not_isolated_cores.remove(core) {
            bail!(
                "Core {} does not exist or was specified twice in the list of isolated cores",
                core
            );
        }
    }

    if not_isolated_cores.is_empty() {
        bail!("Cannot isolate all cores, at least one core should be devoted to general purpose tasks");
    }

    let not_isolated_cores = format_cpuset_list(Vec::from_iter(not_isolated_cores.iter().cloned()));

    std::fs::write(
        format!("{}/sunwalker_root/cpuset.cpus", CPUSET_ROOT),
        &not_isolated_cores,
    )
    .with_context(|| {
        format!(
            "Failed to write {} to {}/sunwalker_root/cpuset.cpus",
            not_isolated_cores, CPUSET_ROOT
        )
    })
}
