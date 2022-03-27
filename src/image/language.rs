use crate::{
    image::{config, package},
    system,
};
use anyhow::{anyhow, Context, Result};
use rand::{thread_rng, Rng};
use std::path::PathBuf;
use std::process::{Command, Stdio};

pub struct Language<'a> {
    pub package: &'a package::Package<'a>,
    pub config: &'a config::Language,
    pub name: &'a str,
}

impl Language<'_> {
    pub fn identify(&self, sandbox_config: &package::SandboxConfig) -> Result<String> {
        // Make sandbox
        self.package
            .make_worker_tmp(&sandbox_config)
            .with_context(|| format!("Failed to make /tmp/worker for identification"))?;
        self.package
            .make_sandbox(&sandbox_config)
            .with_context(|| format!("Failed to make sandbox for identification"))?;

        std::fs::write("/tmp/worker/overlay/identify.txt", "")?;
        std::os::unix::fs::chown("/tmp/worker/overlay/identify.txt", Some(65534), Some(65534))?;

        // Enter the sandbox in another process
        self.package
            .run_in_sandbox(&sandbox_config, || {
                // Evaluate correct pattern
                let identify: String =
                    lisp::evaluate(self.config.identify.clone(), &lisp::State::new())
                        .unwrap()
                        .to_native()
                        .unwrap();

                // Output the pattern to /identify.txt
                std::fs::write("/identify.txt", identify)
                    .with_context(|| "Failed to write pattern to /identify.txt")
                    .unwrap();
            })
            .with_context(|| "In-process build failed")?;

        let identify = std::fs::read_to_string("/tmp/worker/overlay/identify.txt")
            .with_context(|| "Failed to read pattern from /tmp/worker/overlay/identify.txt")?;

        self.package.remove_sandbox()?;

        Ok(identify)
    }

    pub fn build(
        &self,
        mut input_files: Vec<&str>,
        sandbox_config: &package::SandboxConfig,
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
            .make_worker_tmp(&build_sandbox_config)
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

        // Allow the sandbox user to access data
        std::os::unix::fs::chown("/tmp/worker/overlay/artifacts", Some(65534), Some(65534))?;

        // Enter the sandbox in another process
        self.package
            .run_in_sandbox(&build_sandbox_config, || {
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
                            .with_context(|| {
                                format!("Failed to create file {:?} on overlay", new_path)
                            })
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

                // TODO: log?
                // println!("build output: {}", build_output);

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
            .with_context(|| "In-process build failed")?;

        self.package.remove_sandbox()?;

        let pattern = std::fs::read_to_string("/tmp/worker/artifacts/pattern.txt")
            .with_context(|| "Failed to read pattern from /artifacts/pattern.txt")?;

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

    pub fn get_ready_to_run(&self, sandbox_config: &package::SandboxConfig) -> Result<()> {
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

    pub fn run(&self, sandbox_config: &package::SandboxConfig, program: &Program) -> Result<()> {
        let mut run_sandbox_config = sandbox_config.clone();
        for prerequisite in &program.prerequisites {
            run_sandbox_config.bound_files.push((
                format!("/tmp/worker/artifacts/{}", prerequisite).into(),
                format!("/space/{}", prerequisite),
            ));
        }

        // Enter the sandbox in another process
        self.package
            .run_in_sandbox(&sandbox_config, || {
                std::env::set_current_dir("/space")
                    .with_context(|| "Failed to chdir to /space")
                    .unwrap();

                std::process::Command::new(&program.argv[0])
                    .args(&program.argv[1..])
                    .spawn()
                    .with_context(|| format!("Failed to spawn {:?}", program.argv))
                    .unwrap()
                    .wait()
                    .with_context(|| format!("Failed to get exit code of {:?}", program.argv))
                    .unwrap();
            })
            .with_context(|| "In-process running failed")?;

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
            String::from_utf8_lossy(&output.stdout).into_owned()
                + &String::from_utf8_lossy(&output.stderr), // TODO: interleave
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

#[lisp::function]
fn mv(call: lisp::CallTerm, state: &lisp::State) -> Result<lisp::TypedRef, lisp::Error> {
    let argv = lisp::builtins::as_tuple2(call)?;
    let from: String = lisp::evaluate(argv.0, state)?.to_native()?;
    let to: String = lisp::evaluate(argv.1, state)?.to_native()?;
    match std::fs::rename(&from, &to) {
        Ok(()) => Ok(lisp::TypedRef::new(())),
        Err(e) => Err(lisp::Error {
            message: format!("Failed to move file {:?} to {:?}: {}", from, to, e),
        }),
    }
}
