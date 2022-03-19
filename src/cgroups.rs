use anyhow::{Context, Result};
use libc::pid_t;
use std::io::Write;


pub fn create_root_cpuset() -> Result<()> {
	std::fs::create_dir("/sys/fs/cgroup/cpuset/sunwalker_root")
		.or_else(|e| {
			if e.kind() == std::io::ErrorKind::AlreadyExists {
				Ok(())
			} else {
				Err(e)
			}
		})
		.with_context(|| "Unable to create /sys/fs/cgroup/cpuset/sunwalker_root directory")?;

	// Move all tasks that don't yet belong to a cpuset here
	let mut pids: Vec<pid_t> = Vec::new();
	for entry in std::fs::read_dir("/proc")? {
		let entry = entry?;
		let name = entry.file_name();
		if let Ok(name) = name.into_string() {
			if let Ok(pid) = name.parse::<pid_t>() {
				let mut cpuset_path = entry.path();
				cpuset_path.push("cpuset");
				let cpuset = std::fs::read_to_string(cpuset_path)?;
				if cpuset == "/\n" {
					// Does not belong to any cpuset yet
					pids.push(pid);
				} else {
					// TODO: issue a warning or something
				}
			}
		}
	}

	let mut tasks_file = std::fs::OpenOptions::new().read(true).write(true).open("/sys/fs/cgroup/cpuset/sunwalker_root/tasks")
		.with_context(|| "Cannot open /sys/fs/cgroup/cpuset/sunwalker_root/tasks for writing")?;
	for pid in pids {
		tasks_file.write_all(pid.to_string().as_ref())
			.with_context(|| format!("Cannot write PID {} to /sys/fs/cgroup/cpuset/sunwalker_root/tasks", pid))?;
	}

	Ok(())
}
