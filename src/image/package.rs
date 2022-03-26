use crate::{
    image::{config, mount},
    process, system,
};
use anyhow::{anyhow, bail, Context, Result};
use libc::{CLONE_NEWIPC, CLONE_NEWNET, CLONE_NEWNS};
use rand::{thread_rng, Rng};
use std::io::BufRead;
use std::path::PathBuf;
use std::process::{Command, Stdio};

#[derive(Clone)]
pub struct SandboxConfig {
    pub max_size_in_bytes: u64,
    pub max_inodes: u64,
    pub bound_files: Vec<(PathBuf, String)>,
}

pub struct Package<'a> {
    image: &'a mount::MountedImage,
    name: &'a str,
}

pub struct Language<'a> {
    package: &'a Package<'a>,
    config: &'a config::Language,
    name: &'a str,
}

impl<'a> Package<'a> {
    pub fn new(image: &'a mount::MountedImage, name: &'a str) -> Result<Package<'a>> {
        if !image.has_package(name.as_ref()) {
            bail!("Image {:?} does not contain package {}", image, name);
        }
        Ok(Package { image, name })
    }

    pub fn make_worker_tmp(&self, sandbox_config: &SandboxConfig) -> Result<()> {
        // Unshare namespaces
        unsafe {
            if libc::unshare(CLONE_NEWNS) != 0 {
                bail!("Could not unshare mount namespace");
            }
        }

        // Create per-worker tmpfs
        system::mount(
            "none",
            "/tmp/worker",
            "tmpfs",
            0,
            Some(
                format!(
                    "size={},nr_inodes={}",
                    sandbox_config.max_size_in_bytes, sandbox_config.max_inodes
                )
                .as_ref(),
            ),
        )
        .with_context(|| "Mounting tmpfs on /tmp/worker failed")?;

        Ok(())
    }

    pub fn make_sandbox(&self, sandbox_config: &SandboxConfig) -> Result<()> {
        std::fs::create_dir("/tmp/worker/user-area")?;
        std::fs::create_dir("/tmp/worker/work")?;
        std::fs::create_dir("/tmp/worker/overlay")?;

        // Mount overlay
        sys_mount::Mount::builder()
            .fstype("overlay")
            .data(
                format!(
                    "lowerdir={}/{},upperdir=/tmp/worker/user-area,workdir=/tmp/worker/work",
                    self.image
                        .mountpoint
                        .to_str()
                        .expect("Mountpoint must be a string"),
                    self.name
                )
                .as_ref(),
            )
            .mount("overlay", "/tmp/worker/overlay")
            .with_context(|| "Failed to mount overlay")?;

        // Initialize user directory
        std::fs::create_dir("/tmp/worker/overlay/space")
            .with_context(|| "Failed to create .../space")?;
        for (from, to) in &sandbox_config.bound_files {
            let to = format!("/tmp/worker/overlay{}", to);
            std::fs::write(&to, "")
                .with_context(|| format!("Failed to create file {:?} on overlay", to))?;
            system::bind_mount_opt(&from, &to, system::MS_RDONLY).with_context(|| {
                format!("Failed to bind-mount {:?} -> {:?} on overlay", from, to)
            })?;
        }

        // Mount /dev on overlay
        std::fs::create_dir("/tmp/worker/overlay/dev")
            .with_context(|| "Failed to create .../dev")?;
        system::bind_mount_opt("/tmp/dev", "/tmp/worker/overlay/dev", system::MS_RDONLY)
            .with_context(|| "Failed to mount /dev on overlay")?;

        Ok(())
    }

    pub fn remove_sandbox(&self) -> Result<()> {
        // Unmount overlay recursively
        let file = std::fs::File::open("/proc/self/mounts")
            .with_context(|| "Could not open /proc/self/mounts for reading")?;
        let mut vec = Vec::new();
        for line in std::io::BufReader::new(file).lines() {
            let line = line?;
            let mut it = line.split(" ");
            it.next()
                .ok_or_else(|| anyhow!("Invalid format of /proc/self/mounts"))?;
            let target_path = it
                .next()
                .ok_or_else(|| anyhow!("Invalid format of /proc/self/mounts"))?;
            if target_path.starts_with("/tmp/worker/overlay") {
                vec.push(target_path.to_string());
            }
        }
        for path in vec.into_iter().rev() {
            system::umount(&path).with_context(|| format!("Failed to unmount {}", path))?;
        }

        // Remove directories
        std::fs::remove_dir_all("/tmp/worker/user-area")?;
        std::fs::remove_dir_all("/tmp/worker/work")?;
        std::fs::remove_dir_all("/tmp/worker/overlay")?;

        Ok(())
    }

    pub fn enter_sandbox(&self, sandbox_config: &SandboxConfig) -> Result<()> {
        // Unshare namespaces
        unsafe {
            // TODO: CLONE_NEWPID, CLONE_NEWUSER?, CLONE_NEWUTS, CLONE_SYSVSEM
            if libc::unshare(CLONE_NEWNS | CLONE_NEWIPC | CLONE_NEWNET) != 0 {
                bail!("Could not unshare mount namespace");
            }
        }

        // Change root
        std::fs::create_dir("/tmp/worker/overlay/old-root")
            .with_context(|| "Failed to create .../old-root")?;
        std::env::set_current_dir("/tmp/worker/overlay")
            .with_context(|| "Failed to chdir to new root")?;
        nix::unistd::pivot_root("/tmp/worker/overlay", "/tmp/worker/overlay/old-root")
            .with_context(|| "Failed to pivot_root")?;
        system::umount_opt("/old-root", system::MNT_DETACH)
            .with_context(|| "Failed to unmount /old-root")?;
        std::fs::remove_dir("/old-root").with_context(|| "Failed to remove .../old-root")?;

        // Expose defaults for environment variables
        std::env::set_var(
            "LD_LIBRARY_PATH",
            "/usr/local/lib64:/usr/local/lib:/usr/lib64:/usr/lib:/lib64:/lib",
        );
        std::env::set_var("LANGUAGE", "en_US");
        std::env::set_var("LC_ALL", "en_US.UTF-8");
        std::env::set_var("LC_ADDRESS", "en_US.UTF-8");
        std::env::set_var("LC_NAME", "en_US.UTF-8");
        std::env::set_var("LC_MONETARY", "en_US.UTF-8");
        std::env::set_var("LC_PAPER", "en_US.UTF-8");
        std::env::set_var("LC_IDENTIFIER", "en_US.UTF-8");
        std::env::set_var("LC_TELEPHONE", "en_US.UTF-8");
        std::env::set_var("LC_MEASUREMENT", "en_US.UTF-8");
        std::env::set_var("LC_TIME", "en_US.UTF-8");
        std::env::set_var("LC_NUMERIC", "en_US.UTF-8");
        std::env::set_var("LANG", "en_US.UTF-8");

        // Use environment from the package
        let file = std::fs::File::open("/.sunwalker/env")
            .with_context(|| "Could not open /.sunwalker/env for reading")?;
        for line in std::io::BufReader::new(file).lines() {
            let line = line.with_context(|| "Could not read from /.sunwalker/env")?;
            let idx = line
                .find('=')
                .with_context(|| format!("'=' not found in a line of /.sunwalker/env: {}", line))?;
            let (name, value) = line.split_at(idx);
            let value = &value[1..];
            std::env::set_var(name, value);
        }

        Ok(())
    }

    pub fn get_language(&'a self, language_name: &'a str) -> Result<Language<'a>> {
        let package = self
            .image
            .config
            .packages
            .get(self.name)
            .with_context(|| format!("Package {} not found in the image", self.name))?;
        Ok(Language {
            package: &self,
            config: package.languages.get(language_name).with_context(|| {
                format!(
                    "Packages {} does not provide language {}",
                    self.name, language_name
                )
            })?,
            name: language_name,
        })
    }
}

impl Language<'_> {
    pub fn build(
        &self,
        mut input_files: Vec<&str>,
        sandbox_config: &SandboxConfig,
    ) -> Result<Program> {
        // Map input files to patterned filenames based on extension
        let mut patterns_by_extension = Vec::new();
        for input_pattern in &self.config.inputs {
            let suffix = input_pattern.rsplit_once("%").ok_or_else(|| {
                anyhow!(
                    "Input file pattern {} (derived from Makefile of package {}, language {}) does not contain glob character %",
                    input_pattern, self.package.name, self.name
                )
            })?.1;
            patterns_by_extension.push((input_pattern, suffix));
        }

        // Sort by suffix lengths (decreasing)
        patterns_by_extension.sort_unstable_by(|a, b| b.1.len().cmp(&a.1.len()));

        // Rename files appropriately
        let mut files_and_patterns = Vec::new();
        for (input_pattern, suffix) in patterns_by_extension.into_iter() {
            let i = input_files
                .iter()
                .enumerate()
                .find(|(_, input_file)| input_file.ends_with(suffix))
                .ok_or_else(|| {
                    anyhow!(
                        "No input file ends with {} (derived from pattern {}). This requirement is because language {} accepts multiple input files.",
                        suffix,
                        input_pattern,
                        self.name
                    )
                })?.0;
            let input_file = input_files.remove(i);
            files_and_patterns.push((input_file, input_pattern));
        }

        // Set pattern arbitrarily
        let mut pre_pattern = [0i8; 8];
        thread_rng().fill(&mut pre_pattern[..]);
        let pre_pattern = pre_pattern.map(|x| format!("{:02x}", x)).join("");

        // Mount input files into sandbox
        let mut build_sandbox_config = sandbox_config.clone();
        for (input_file, input_pattern) in &files_and_patterns {
            build_sandbox_config.bound_files.push((
                input_file.into(),
                "/space/".to_string() + &input_pattern.replace("%", &pre_pattern),
            ));
        }

        // Make sandbox
        self.package
            .make_worker_tmp(&sandbox_config)
            .with_context(|| format!("Failed to make /tmp/worker for build"))?;
        self.package
            .make_sandbox(&build_sandbox_config)
            .with_context(|| format!("Failed to make sandbox for build"))?;

        // Add /artifacts -> /tmp/worker/artifacts
        std::fs::create_dir("/tmp/worker/artifacts")
            .with_context(|| "Could not create /tmp/worker/artifacts")?;
        std::fs::create_dir("/tmp/worker/overlay/artifacts")
            .with_context(|| "Could not create /tmp/worker/overlay/artifacts")?;
        system::bind_mount("/tmp/worker/artifacts", "/tmp/worker/overlay/artifacts")?;
        build_sandbox_config
            .bound_files
            .push(("/tmp/worker/artifacts".into(), "/artifacts".into()));

        // Enter the sandbox in another process
        process::scoped(|| {
            self.package
                .enter_sandbox(&build_sandbox_config)
                .with_context(|| format!("Failed to enter sandbox for build"))
                .unwrap();

            // Evaluate correct pattern
            let pattern: String = lisp::evaluate(
                self.config.base_rule.clone(),
                &lisp::State::new().var("$base".to_string(), pre_pattern.clone()),
            )
            .unwrap()
            .to_native()
            .unwrap();

            if pre_pattern != pattern {
                // Rename files according to new pattern
                for (_, input_pattern) in files_and_patterns {
                    let mut old_path = PathBuf::new();
                    old_path.push("/space");
                    old_path.push(input_pattern.replace("%", &pre_pattern));

                    let mut new_path = PathBuf::new();
                    new_path.push("/space");
                    new_path.push(input_pattern.replace("%", &pattern));

                    std::fs::write(&new_path, "")
                        .with_context(|| format!("Failed to create file {:?} on overlay", new_path))
                        .unwrap();
                    system::move_mount(&old_path, &new_path)
                        .with_context(|| {
                            format!(
                                "Failed to move mount {:?} -> {:?} on overlay",
                                &old_path, new_path
                            )
                        })
                        .unwrap();
                    std::fs::remove_file(&old_path)
                        .with_context(|| {
                            format!("Failed to remove old file {:?} on overlay", old_path)
                        })
                        .unwrap();
                }
            }

            // Run build process
            let state = lisp::State::new().var("$base".to_string(), pattern.clone());
            let build_output: String = lisp::evaluate(self.config.build.clone(), &state)
                .with_context(|| "Failed to evaluate build schema")
                .unwrap()
                .to_native()
                .with_context(|| "Build schema didn't return string, as was expected")
                .unwrap();

            let run_prerequisites: Vec<String> =
                lisp::evaluate(self.config.run.prerequisites.clone(), &state)
                    .with_context(|| "Failed to evaluate prerequisites for running")
                    .unwrap()
                    .to_native()
                    .with_context(|| {
                        "Prerequisite schema didn't return a list of strings, as was expected"
                    })
                    .unwrap();

            // Copy run prerequisites to artifacts.
            // TODO: this can be optimized further. If a prerequisite is an artifact of the build
            // process, the file can simply be moved. If it is an input file, it can be bind-mounted
            // from its original source.
            for rel_path in run_prerequisites.into_iter() {
                let mut from = std::path::PathBuf::from("/space");
                from.push(&rel_path);
                let mut to = std::path::PathBuf::from("/artifacts");
                to.push(&rel_path);
                std::fs::copy(&from, &to)
                    .with_context(|| {
                        format!(
                            "Could not copy artifact {} from {:?} to {:?}",
                            rel_path, from, to
                        )
                    })
                    .unwrap();
            }

            // Output the pattern to /artifacts/pattern.txt
            std::fs::write("/artifacts/pattern.txt", pattern)
                .with_context(|| "Failed to write pattern to /artifacts/pattern.txt")
                .unwrap();
        })
        .with_context(|| "In-process build failed")?
        .join()?;

        self.package.remove_sandbox()?;

        let pattern = std::fs::read_to_string("/tmp/worker/artifacts/pattern.txt")
            .with_context(|| "Failed to read pattern from /artifacts/pattern.txt")
            .unwrap();

        // TODO: log?
        // println!("build output: {}", build_output);

        let prerequisites: Vec<String> = lisp::evaluate(
            self.config.run.prerequisites.clone(),
            &lisp::State::new().var("$base".to_string(), pattern.clone()),
        )
        .with_context(|| "Failed to evaluate run.prerequisites")?
        .to_native()
        .with_context(|| {
            "Failed to parse prerequisites generated by the schema as vector of strings"
        })?;

        let argv: Vec<String> = lisp::evaluate(
            self.config.run.argv.clone(),
            &lisp::State::new().var("$base".to_string(), pattern.clone()),
        )
        .with_context(|| "Failed to evaluate run.argv")?
        .to_native()
        .with_context(|| "Failed to parse argv generated by the schema as vector of strings")?;

        Ok(Program {
            prerequisites,
            argv,
        })
    }

    pub fn get_ready_to_run(&self, sandbox_config: &SandboxConfig) -> Result<()> {
        let mut run_sandbox_config = sandbox_config.clone();

        for artifact_entry in std::fs::read_dir("/tmp/worker/artifacts")? {
            let artifact_entry = artifact_entry?;
            let artifact_name = artifact_entry
                .file_name()
                .into_string()
                .map_err(|e| anyhow!("Failed to parse artifact name {:?} as UTF-8 string", e))?;
            run_sandbox_config.bound_files.push((
                format!("/tmp/worker/artifacts/{}", artifact_name).into(),
                format!("/space/{}", artifact_name).into(),
            ));
        }

        self.package
            .make_sandbox(&run_sandbox_config)
            .with_context(|| "Failed to make sandbox for running")?;

        Ok(())
    }

    pub fn run(&self, sandbox_config: &SandboxConfig, program: &Program) -> Result<()> {
        let mut run_sandbox_config = sandbox_config.clone();
        for prerequisite in &program.prerequisites {
            run_sandbox_config.bound_files.push((
                format!("/tmp/worker/artifacts/{}", prerequisite).into(),
                format!("/space/{}", prerequisite),
            ));
        }

        // Enter the sandbox in another process
        process::scoped(|| {
            self.package
                .enter_sandbox(&run_sandbox_config)
                .with_context(|| "Failed to enter sandbox for running")
                .unwrap();

            std::env::set_current_dir("/space")
                .with_context(|| "Failed to chdir to /space")
                .unwrap();

            (Err(exec::Command::new(&program.argv[0])
                .args(&program.argv[1..])
                .exec()) as Result<i64, exec::Error>)
                .with_context(|| format!("Failed to start {:?}", program.argv))
                .unwrap();
        })
        .with_context(|| "In-process running failed")?
        .join()?;

        Ok(())
    }
}

pub struct Program {
    prerequisites: Vec<String>,
    argv: Vec<String>,
}

#[lisp::function]
fn exec(call: lisp::CallTerm, state: &lisp::State) -> Result<lisp::TypedRef, lisp::Error> {
    let argv: Vec<String> = lisp::evaluate(lisp::builtins::as_item1(call)?, state)?.to_native()?;
    let output = Command::new(argv[0].clone())
        .args(argv.iter().skip(1))
        .stdin(Stdio::null())
        .current_dir("/space")
        .output()
        .map_err(|e| lisp::Error {
            message: format!("Failed to start process {:?}: {}", argv, e),
        })?;
    if output.status.success() {
        Ok(lisp::TypedRef::new(
            String::from_utf8_lossy(&output.stdout).into_owned(),
        ))
    } else {
        Err(lisp::Error {
            message: format!(
                "Process {:?} failed: {}\n\n{}\n\n{}",
                argv,
                output.status,
                String::from_utf8_lossy(&output.stdout).into_owned(),
                String::from_utf8_lossy(&output.stderr).into_owned()
            ),
        })
    }
}
