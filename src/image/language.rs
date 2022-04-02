use crate::{
    errors,
    image::{config, package, program, sandbox},
    system,
};
use anyhow::Context;
use ouroboros::self_referencing;
use rand::{thread_rng, Rng};
use serde::{ser::SerializeStruct, Deserialize, Deserializer, Serialize, Serializer};
use std::path::PathBuf;
use std::process::{Command, Stdio};

#[self_referencing(pub_extras)]
pub struct LanguageImpl {
    package: package::Package,
    #[borrows(package)]
    config: &'this config::Language,
    name: String,
}

impl LanguageImpl {
    pub async fn identify(
        &self,
        worker_space: &sandbox::WorkerSpace,
    ) -> Result<String, errors::Error> {
        let package = self.borrow_package();

        // Make sandbox
        let rootfs = worker_space.make_rootfs(package, Vec::new()).map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to make sandbox for identification: {:?}",
                e
            ))
        })?;

        std::fs::write("/tmp/worker/overlay/identify.txt", "").map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to create /tmp/worker/overlay/identify.txt: {:?}",
                e
            ))
        })?;
        std::os::unix::fs::chown("/tmp/worker/overlay/identify.txt", Some(65534), Some(65534))
            .map_err(|e| {
                errors::InvokerFailure(format!(
                    "Failed to chown /tmp/worker/overlay/identify.txt: {:?}",
                    e
                ))
            })?;

        // Enter the sandbox in another process
        rootfs
            .run_isolated(|| {
                // Evaluate correct pattern
                let identify: String =
                    lisp::evaluate(self.borrow_config().identify.clone(), &lisp::State::new())
                        .unwrap()
                        .to_native()
                        .unwrap();

                // Output the pattern to /identify.txt
                std::fs::write("/identify.txt", identify)
                    .with_context(|| "Failed to write pattern to /identify.txt")
                    .unwrap();
            })
            .await?;

        let identify =
            std::fs::read_to_string("/tmp/worker/overlay/identify.txt").map_err(|e| {
                errors::InvokerFailure(format!(
                    "Identification succeeded, but did not generate a readable file at \
                     /tmp/worker/overlay/identify.txt: {:?}",
                    e
                ))
            })?;

        if identify == "" {
            return Err(errors::InvokerFailure(
                "Identification succeeded, but did not write anything to
                 /tmp/worker/overlay/identify.txt"
                    .to_string(),
            ));
        }

        rootfs
            .remove()
            .map_err(|e| errors::InvokerFailure(format!("Failed to remove rootfs: {:?}", e)))?;

        Ok(identify)
    }

    pub async fn build(
        &self,
        mut input_files: Vec<&str>,
        worker_space: &sandbox::WorkerSpace,
    ) -> Result<program::Program, errors::Error> {
        let package = self.borrow_package();
        let config = self.borrow_config();
        let name = self.borrow_name();

        // Map input files to patterned filenames based on extension
        let mut patterns_by_extension = Vec::new();
        for input_pattern in &config.inputs {
            let suffix = input_pattern
                .rsplit_once("%")
                .ok_or_else(|| {
                    errors::InvokerFailure(format!(
                        "Input file pattern {} (derived from Makefile of package {}, language {}) \
                         does not contain glob character %",
                        input_pattern, package.name, name
                    ))
                })?
                .1;
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
                    errors::UserFailure(format!(
                        "No input file ends with {} (derived from pattern {}). This requirement \
                         is because language {} accepts multiple input files.",
                        suffix, input_pattern, name
                    ))
                })?
                .0;
            let input_file = input_files.remove(i);
            files_and_patterns.push((input_file, input_pattern));
        }

        // Set pattern arbitrarily
        let mut pre_pattern = [0i8; 8];
        thread_rng().fill(&mut pre_pattern[..]);
        let pre_pattern = pre_pattern.map(|x| format!("{:02x}", x)).join("");

        // Mount input files into sandbox
        let mut bound_files = Vec::new();
        for (input_file, input_pattern) in &files_and_patterns {
            bound_files.push((
                input_file.into(),
                "/space/".to_string() + &input_pattern.replace("%", &pre_pattern),
            ));
        }

        // Make sandbox
        let rootfs = worker_space
            .make_rootfs(package, bound_files)
            .map_err(|e| {
                errors::InvokerFailure(format!("Failed to make sandbox for build: {:?}", e))
            })?;

        // Add /artifacts -> /tmp/worker/artifacts
        std::fs::create_dir("/tmp/worker/artifacts").map_err(|e| {
            errors::InvokerFailure(format!("Failed to create /tmp/worker/artifacts: {:?}", e))
        })?;
        std::fs::create_dir("/tmp/worker/overlay/artifacts").map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to create /tmp/worker/overlay/artifacts: {:?}",
                e
            ))
        })?;
        system::bind_mount("/tmp/worker/artifacts", "/tmp/worker/overlay/artifacts").map_err(
            |e| {
                errors::InvokerFailure(format!(
                    "Failed to bind-mount /tmp/worker/overlay/artifacts: {:?}",
                    e
                ))
            },
        )?;

        // Allow the sandbox user to access data
        std::os::unix::fs::chown("/tmp/worker/overlay/artifacts", Some(65534), Some(65534))
            .map_err(|e| {
                errors::InvokerFailure(format!(
                    "Failed to chown /tmp/worker/overlay/artifacts: {:?}",
                    e
                ))
            })?;

        // Enter the sandbox in another process
        rootfs
            .run_isolated(|| {
                // Evaluate correct pattern
                let pattern: String = lisp::evaluate(
                    config.base_rule.clone(),
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
                let build_output: String = lisp::evaluate(config.build.clone(), &state)
                    .with_context(|| "Failed to evaluate build schema")
                    .unwrap()
                    .to_native()
                    .with_context(|| "Build schema didn't return string, as was expected")
                    .unwrap();

                // TODO: log?
                // println!("build output: {}", build_output);

                let run_prerequisites: Vec<String> =
                    lisp::evaluate(config.run.prerequisites.clone(), &state)
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
                                "Failed to copy artifact {} from {:?} to {:?}",
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
            .await?;

        rootfs
            .remove()
            .map_err(|e| errors::InvokerFailure(format!("Failed to remove rootfs: {:?}", e)))?;

        let pattern =
            std::fs::read_to_string("/tmp/worker/artifacts/pattern.txt").map_err(|e| {
                errors::InvokerFailure(format!(
                    "Failed to read pattern from /artifacts/pattern.txt: {:?}",
                    e
                ))
            })?;

        let prerequisites: Vec<String> = lisp::evaluate(
            config.run.prerequisites.clone(),
            &lisp::State::new().var("$base".to_string(), pattern.clone()),
        )
        .map_err(|e| {
            errors::InvokerFailure(format!("Failed to evaluate run.prerequisites: {:?}", e))
        })?
        .to_native()
        .map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to parse prerequisites generated by the schema as vector of strings: {:?}",
                e
            ))
        })?;

        let argv: Vec<String> = lisp::evaluate(
            config.run.argv.clone(),
            &lisp::State::new().var("$base".to_string(), pattern.clone()),
        )
        .map_err(|e| errors::InvokerFailure(format!("Failed to evaluate run.argv: {:?}", e)))?
        .to_native()
        .map_err(|e| {
            errors::InvokerFailure(format!(
                "Failed to parse argv generated by the schema as vector of strings: {:?}",
                e
            ))
        })?;

        Ok(program::Program {
            package: self.borrow_package().clone(),
            prerequisites,
            argv,
        })
    }

    pub async fn run(
        &self,
        worker_space: &sandbox::WorkerSpace,
        program: &program::Program,
    ) -> Result<(), errors::Error> {
        program.run(worker_space).await
    }
}

pub struct Language {
    nested: LanguageImpl,
}

impl Language {
    pub fn new(package: package::Package, name: &str) -> anyhow::Result<Language> {
        package
            .image
            .config
            .packages
            .get(&package.name)
            .with_context(|| format!("Package {} not found in the image", package.name))?
            .languages
            .get(name)
            .with_context(|| {
                format!(
                    "Packages {} does not provide language {}",
                    package.name, name
                )
            })?;

        Ok(Language {
            nested: LanguageImpl::new(
                package,
                |package| {
                    package
                        .image
                        .config
                        .packages
                        .get(&package.name)
                        .unwrap()
                        .languages
                        .get(name)
                        .unwrap()
                },
                name.to_string(),
            ),
        })
    }

    pub async fn identify(
        &self,
        worker_space: &sandbox::WorkerSpace,
    ) -> Result<String, errors::Error> {
        self.nested.identify(worker_space).await
    }

    pub async fn build(
        &self,
        input_files: Vec<&str>,
        worker_space: &sandbox::WorkerSpace,
    ) -> Result<program::Program, errors::Error> {
        self.nested.build(input_files, worker_space).await
    }

    pub async fn run(
        &self,
        worker_space: &sandbox::WorkerSpace,
        program: &program::Program,
    ) -> Result<(), errors::Error> {
        self.nested.run(worker_space, program).await
    }
}

impl Clone for Language {
    fn clone(&self) -> Language {
        Language::new(
            self.nested.borrow_package().clone(),
            self.nested.borrow_name(),
        )
        .expect("Failed to clone a language")
    }
}

impl Serialize for Language {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Language", 2)?;
        state.serialize_field("package", self.nested.borrow_package())?;
        state.serialize_field("name", self.nested.borrow_name())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for Language {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Language, D::Error> {
        let (package, name) = {
            #[derive(Deserialize)]
            struct Language {
                package: package::Package,
                name: String,
            }
            let language = <Language as Deserialize<'de>>::deserialize(deserializer)?;
            (language.package, language.name)
        };
        Ok(Language::new(package, &name).expect("Failed to deserialize a language"))
    }
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
