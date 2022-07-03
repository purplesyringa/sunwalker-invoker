use crate::{
    errors,
    errors::{ToError, ToResult},
    image::{config, package, program, sandbox},
    system,
};
use multiprocessing::{Bind, Deserialize, DeserializeBoxed, Deserializer, Serialize, Serializer};
use ouroboros::self_referencing;
use rand::{thread_rng, Rng};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

#[self_referencing(pub_extras)]
pub struct LanguageImpl {
    package: package::Package,
    #[borrows(package)]
    config: &'this config::Language,
    name: String,
}

impl LanguageImpl {
    pub async fn identify(&self) -> Result<String, errors::Error> {
        let package = self.borrow_package();

        // Make sandbox
        let rootfs = sandbox::make_rootfs(
            package,
            Vec::new(),
            sandbox::DiskQuotas {
                // It should be read-only anyway
                space: 4096,
                max_inodes: 16,
            },
            "identify".to_string(),
        )
        .context_invoker("Failed to make sandbox for identification")?;
        let ns = sandbox::make_namespace("identify".to_string()).await?;

        rootfs
            .reset()
            .context_invoker("Failed to reset rootfs for identification")?;

        // Enter the sandbox in another process
        let identification = sandbox::run_isolated(
            Box::new(identify.bind(self.borrow_config().identify.clone())),
            &rootfs,
            &ns,
        )
        .await?;

        if identification == "" {
            return Err(errors::ConfigurationFailure(
                "Identification succeeded, but did not report anything".to_string(),
            ));
        }

        rootfs.remove().context_invoker("Failed to remove rootfs")?;

        ns.remove().context_invoker("Failed to remove namespace")?;

        Ok(identification)
    }

    pub async fn build(
        &self,
        mut input_files: Vec<&str>,
        build_id: String,
    ) -> Result<(program::Program, String), errors::Error> {
        let package = self.borrow_package();
        let config = self.borrow_config();
        let name = self.borrow_name();

        let mut files_and_patterns: Vec<(&str, &str)> = Vec::new();

        if config.inputs.len() == 1 {
            if input_files.len() != 1 {
                return Err(errors::UserFailure(format!(
                    "There must be exactly one input file, preferably of pattern {}, but {} files \
                     were provided. This requirement is because language {name} accepts exactly \
                     one input file.",
                    config.inputs[0],
                    input_files.len()
                )));
            }

            files_and_patterns.push((input_files[0], &config.inputs[0]));
        } else {
            // Map input files to patterned filenames based on extension
            let mut patterns_by_extension = Vec::new();
            for input_pattern in &config.inputs {
                let suffix = input_pattern
                    .rsplit_once("%")
                    .ok_or_else(|| {
                        errors::ConfigurationFailure(format!(
                            "Input file pattern {input_pattern} (derived from Makefile of package \
                             {}, language {name}) does not contain glob character %",
                            package.name
                        ))
                    })?
                    .1;
                patterns_by_extension.push((input_pattern, suffix));
            }

            // Sort by suffix lengths (decreasing)
            patterns_by_extension.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

            // Rename files appropriately
            for (input_pattern, suffix) in patterns_by_extension.into_iter() {
                let i = input_files
                    .iter()
                    .enumerate()
                    .find(|(_, input_file)| input_file.ends_with(suffix))
                    .ok_or_else(|| {
                        errors::UserFailure(format!(
                            "No input file ends with {suffix} (derived from pattern \
                             {input_pattern}). This requirement is because language {name} \
                             accepts multiple input files."
                        ))
                    })?
                    .0;
                let input_file = input_files.remove(i);
                files_and_patterns.push((input_file, input_pattern));
            }
        }

        // Set pattern arbitrarily
        let mut pre_pattern = [0i8; 8];
        thread_rng().fill(&mut pre_pattern[..]);
        let pre_pattern = pre_pattern.map(|x| format!("{x:02x}")).join("");

        // Mount input files into sandbox
        let mut bound_files = Vec::new();
        for (input_file, input_pattern) in &files_and_patterns {
            bound_files.push((
                input_file.into(),
                "/space/".to_string() + &input_pattern.replace("%", &pre_pattern),
            ));
        }

        // Make sandbox
        let rootfs = sandbox::make_rootfs(
            package,
            bound_files,
            sandbox::DiskQuotas {
                space: 32 * 1024 * 1024, // TODO: make this configurable
                max_inodes: 1024,
            },
            "build".to_string(),
        )
        .context_invoker("Failed to make sandbox for build")?;
        let ns = sandbox::make_namespace("build".to_string()).await?;

        rootfs
            .reset()
            .context_invoker("Failed to reset rootfs for build")?;

        // Add /space/artifacts -> /tmp/sunwalker_invoker/artifacts/{build_id}
        let artifacts_path = PathBuf::from(format!("/tmp/sunwalker_invoker/artifacts/{build_id}"));
        let overlay_artifacts_path = format!("{}/space/artifacts", rootfs.overlay());
        std::fs::create_dir(&artifacts_path)
            .with_context_invoker(|| format!("Failed to create {artifacts_path:?}"))?;
        std::fs::create_dir(&overlay_artifacts_path)
            .with_context_invoker(|| format!("Failed to create {overlay_artifacts_path}"))?;
        system::bind_mount(&artifacts_path, &overlay_artifacts_path)
            .with_context_invoker(|| format!("Failed to bind-mount {artifacts_path:?}"))?;

        // Allow the sandbox user to access data
        std::os::unix::fs::chown(&overlay_artifacts_path, Some(1), Some(1))
            .with_context_invoker(|| format!("Failed to chown {overlay_artifacts_path}"))?;

        // Enter the sandbox in another process
        let (pattern, log) = sandbox::run_isolated(
            Box::new(
                build.bind((*config).clone()).bind(pre_pattern).bind(
                    files_and_patterns
                        .into_iter()
                        .map(|(_, pattern)| pattern.to_string())
                        .collect(),
                ),
            ),
            &rootfs,
            &ns,
        )
        .await?;

        rootfs.remove().context_invoker("Failed to remove rootfs")?;

        ns.remove().context_invoker("Failed to remove namespace")?;

        let prerequisites: Vec<String> = lisp::evaluate(
            config.run.prerequisites.clone(),
            &lisp::State::new().var("$base".to_string(), pattern.clone()),
        )
        .map_err(|e| {
            errors::ConfigurationFailure(format!("Failed to evaluate run.prerequisites: {e:?}"))
        })?
        .to_native()
        .map_err(|e| {
            errors::ConfigurationFailure(format!(
                "Failed to parse prerequisites generated by the schema as vector of strings: {e:?}"
            ))
        })?;

        let argv: Vec<String> = lisp::evaluate(
            config.run.argv.clone(),
            &lisp::State::new().var("$base".to_string(), pattern.clone()),
        )
        .map_err(|e| errors::ConfigurationFailure(format!("Failed to evaluate run.argv: {e:?}")))?
        .to_native()
        .map_err(|e| {
            errors::ConfigurationFailure(format!(
                "Failed to parse argv generated by the schema as vector of strings: {e:?}"
            ))
        })?;

        Ok((
            program::Program {
                package: self.borrow_package().clone(),
                prerequisites,
                argv,
                artifacts_path,
            },
            log,
        ))
    }
}

pub struct Language {
    nested: LanguageImpl,
}

impl Language {
    pub fn new(package: package::Package, name: &str) -> Result<Language, errors::Error> {
        package
            .image
            .config
            .packages
            .get(&package.name)
            .with_context_invoker(|| format!("Package {} not found in the image", package.name))?
            .languages
            .get(name)
            .with_context_invoker(|| {
                format!("Packages {} does not provide language {name}", package.name)
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

    pub async fn identify(&self) -> Result<String, errors::Error> {
        self.nested.identify().await
    }

    pub async fn build(
        &self,
        input_files: Vec<&str>,
        build_id: String,
    ) -> Result<(program::Program, String), errors::Error> {
        self.nested.build(input_files, build_id).await
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
    fn serialize_self(&self, s: &mut Serializer) {
        s.serialize(self.nested.borrow_package());
        s.serialize(self.nested.borrow_name());
    }
}
impl Deserialize for Language {
    fn deserialize_self(d: &mut Deserializer) -> Self {
        let package = d.deserialize();
        let name: String = d.deserialize();
        Language::new(package, &name).expect("Failed to deserialize a language")
    }
}
impl<'a> DeserializeBoxed<'a> for Language {
    unsafe fn deserialize_on_heap(
        &self,
        d: &mut Deserializer,
    ) -> Box<dyn DeserializeBoxed<'a> + 'a> {
        Box::new(Self::deserialize_self(d))
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
            message: format!("Failed to start process {argv:?}: {e}"),
        })?;
    if output.status.success() {
        Ok(lisp::TypedRef::new(
            String::from_utf8_lossy(&output.stdout).into_owned()
                + &String::from_utf8_lossy(&output.stderr), // TODO: interleave
        ))
    } else {
        Err(lisp::Error {
            message: format!(
                "Process failed: {argv:?}: {}\n\n{}\n\n{}",
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
            message: format!("Failed to move file {from:?} to {to:?}: {e}"),
        }),
    }
}

#[multiprocessing::entrypoint]
fn identify(term: lisp::Term) -> Result<String, errors::Error> {
    lisp::evaluate(term, &lisp::State::new())
        .context_invoker("Failed to identify")?
        .to_native()
        .context_invoker("Failed to interpret identify result as a string")
}

#[multiprocessing::entrypoint]
fn build(
    config: config::Language,
    pre_pattern: String,
    patterns: Vec<String>,
) -> Result<(String, String), errors::Error> {
    // Evaluate correct pattern
    let pattern: String = lisp::evaluate(
        config.base_rule.clone(),
        &lisp::State::new().var("$base".to_string(), pre_pattern.clone()),
    )
    .context_invoker("Failed to evaluate pattern")?
    .to_native()
    .context_invoker("Failed to parse the pattern generated by the schema as a string")?;

    if pre_pattern != pattern {
        // Rename files according to new pattern
        for pattern in patterns {
            let old_path = Path::new("/space").join(pattern.replace("%", &pre_pattern));
            let new_path = Path::new("/space").join(pattern.replace("%", &pattern));
            std::fs::write(&new_path, "").with_context_invoker(|| {
                format!("Failed to create file {new_path:?} on overlay")
            })?;
            system::move_mount(&old_path, &new_path).with_context_invoker(|| {
                format!("Failed to move mount {old_path:?} -> {new_path:?} on overlay")
            })?;
            std::fs::remove_file(&old_path).with_context_invoker(|| {
                format!("Failed to remove old file {old_path:?} on overlay")
            })?;
        }
    }

    // Run build process
    let state = lisp::State::new().var("$base".to_string(), pattern.clone());
    let log: String = lisp::evaluate(config.build.clone(), &state)
        .map_err(|e| {
            if e.message.starts_with("Process failed: ") {
                errors::UserFailure(e.message)
            } else {
                e.context_invoker("Failed to build the program")
            }
        })?
        .to_native()
        .context_invoker("Failed to parse compilation log as a string")?;

    let run_prerequisites: Vec<String> = lisp::evaluate(config.run.prerequisites.clone(), &state)
        .context_invoker("Failed to evaluate run.prerequisites")?
        .to_native()
        .context_invoker(
            "Failed to parse prerequisites generated by the schema as vector of strings",
        )?;

    // Copy run prerequisites to artifacts.
    // TODO: this can be optimized further. If a prerequisite is an artifact of the build
    // process, the file can simply be moved. If it is an input file, it can be bind-mounted
    // from its original source.
    for rel_path in run_prerequisites.into_iter() {
        let from = Path::new("/space").join(&rel_path);
        let to = Path::new("/space/artifacts").join(&rel_path);
        std::fs::copy(&from, &to).with_context_invoker(|| {
            format!("Failed to copy artifact {rel_path} from {from:?} to {to:?}")
        })?;
    }

    Ok((pattern, log))
}
