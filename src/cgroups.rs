use crate::{errors, errors::ToResult};
use libc::pid_t;
use std::collections::HashSet;
use std::path::Path;

pub fn create_root_cpuset() -> Result<(), errors::Error> {
    std::fs::create_dir("/sys/fs/cgroup/sunwalker_root")
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })
        .context_invoker("Unable to create /sys/fs/cgroup/sunwalker_root directory")?;

    std::fs::write(
        "/sys/fs/cgroup/sunwalker_root/cgroup.subtree_control",
        "+cpu +memory +pids +cpuset",
    )
    .context_invoker("Failed to enable cpuset controller")?;

    Ok(())
}

pub fn create_core_cpuset(core: u64) -> Result<(), errors::Error> {
    let dir = format!("/sys/fs/cgroup/sunwalker_root/cpu_{core}");

    std::fs::create_dir(&dir)
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })
        .with_context_invoker(|| format!("Unable to create {dir} directory"))?;

    std::fs::write(
        format!("{dir}/cgroup.subtree_control"),
        "+cpu +memory +pids",
    )
    .with_context_invoker(|| format!("Failed to write to {dir}/cgroup.subtree_control"))?;

    std::fs::write(format!("{dir}/cpuset.cpus"), format!("{core}\n"))
        .with_context_invoker(|| format!("Failed to write to {dir}/cpuset.cpus"))?;

    std::fs::create_dir(format!("{dir}/invoker"))
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })
        .with_context_invoker(|| format!("Unable to create {dir}/invoker directory"))?;

    Ok(())
}

pub fn move_process_to_cgroup(pid: pid_t, name: String) -> Result<(), errors::Error> {
    std::fs::write(
        format!("/sys/fs/cgroup/sunwalker_root/{name}/cgroup.procs"),
        format!("{pid}\n"),
    )
    .with_context_invoker(|| format!("Failed to move process {pid} to cgroup {name}"))
}

pub fn drop_existing_affine_cpusets() -> Result<(), errors::Error> {
    if Path::new("/sys/fs/cgroup/sunwalker_root").exists() {
        // Remove all the child cgroups
        fn cleanup(dir: &Path) -> Result<(), errors::Error> {
            for entry in std::fs::read_dir(dir)
                .with_context_invoker(|| format!("Failed to readdir {dir:?}"))?
            {
                let entry = entry.with_context_invoker(|| format!("Failed to readdir {dir:?}"))?;
                if entry
                    .file_type()
                    .with_context_invoker(|| format!("Failed to stat {:?}", entry.path()))?
                    .is_dir()
                {
                    cleanup(&entry.path())?;
                    std::fs::remove_dir(entry.path())
                        .with_context_invoker(|| format!("Failed to delete {:?}", entry.path()))?;
                }
            }
            Ok(())
        }
        cleanup(Path::new("/sys/fs/cgroup/sunwalker_root"))?;

        let mut backoff = std::time::Duration::from_millis(50);
        let mut times = 0;
        while let Err(e) = std::fs::write(
            "/sys/fs/cgroup/sunwalker_root/cpuset.cpus.partition",
            "member\n",
        ) {
            if let std::io::ErrorKind::ResourceBusy = e.kind() {
                // cgroup operations are asynchronous, so writing to cpuset.cpus.partition right
                // after deleting children may yield EBUSY
                if times == 5 {
                    return Err(e).context_invoker("Failed to make the cgroup a member group");
                }
                std::thread::sleep(backoff);
                backoff *= 2;
                times += 1;
            } else {
                return Err(e).context_invoker("Failed to make the cgroup a member group");
            }
        }
    }

    Ok(())
}

fn parse_cpuset_list(s: &str) -> Result<Vec<u64>, errors::Error> {
    let mut result: Vec<u64> = Vec::new();
    for part in s.trim().split(',') {
        if part.contains('-') {
            let bounds: Vec<&str> = part.split('-').collect();
            if bounds.len() != 2 {
                return Err(errors::InvokerFailure(format!("Invalid cpuset: {part}")));
            }
            let first = bounds[0]
                .parse()
                .with_context_invoker(|| format!("Invalid cpuset: {part}"))?;
            let last = bounds[1]
                .parse()
                .with_context_invoker(|| format!("Invalid cpuset: {part}"))?;
            for item in first..=last {
                result.push(item);
            }
        } else {
            result.push(
                part.parse()
                    .with_context_invoker(|| format!("Invalid cpuset: {part}"))?,
            );
        }
    }
    Ok(result)
}

fn format_cpuset_list(list: &[u64]) -> String {
    list.iter()
        .map(|x| x.to_string())
        .collect::<Vec<String>>()
        .join(",")
}

pub fn isolate_cores(isolated_cores: &[u64]) -> Result<(), errors::Error> {
    if isolated_cores.is_empty() {
        return Err(errors::ConfigurationFailure(
            "Cannot isolate an empty list of cores".to_string(),
        ));
    }

    std::fs::write(
        "/sys/fs/cgroup/sunwalker_root/cpuset.cpus",
        format_cpuset_list(isolated_cores),
    )
    .with_context_invoker(|| {
        format!(
            "Failed to isolate cores {isolated_cores:?}, most likely because they are offline, \
             used by another cgroup, or sunwalker is still running"
        )
    })?;

    let effective_cores: HashSet<u64> = HashSet::from_iter(parse_cpuset_list(
        &std::fs::read_to_string("/sys/fs/cgroup/sunwalker_root/cpuset.cpus.effective")
            .context_invoker("Failed to read cpuset.cpus.effective")?,
    )?);

    let isolated_cores = HashSet::from_iter(isolated_cores.iter().cloned());
    if effective_cores != isolated_cores {
        return Err(errors::InvokerFailure(format!(
            "A subset of cores could not be isolated: {:?}",
            isolated_cores.difference(&effective_cores)
        )));
    }

    if std::fs::read_to_string("/sys/fs/cgroup/sunwalker_root/cpuset.cpus.partition")
        .context_invoker("Failed to read cpuset.cpus.partition")?
        == "member\n"
    {
        std::fs::write(
            "/sys/fs/cgroup/sunwalker_root/cpuset.cpus.partition",
            "root\n",
        )
        .context_invoker(
            "Failed to make the cgroup a root cpuset, most likely because some cores are used by \
             another cgroup",
        )?;
    }

    Ok(())
}
