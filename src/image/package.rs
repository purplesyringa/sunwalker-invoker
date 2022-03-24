use crate::{
    image::{config, mount},
    system,
};
use anyhow::{anyhow, bail, Context, Result};
use libc::CLONE_NEWNS;
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

    pub fn enter(&self, sandbox_config: &SandboxConfig) -> Result<()> {
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
            let to = format!("/tmp/worker/overlay/space/{}", to);
            std::fs::write(&to, "")
                .with_context(|| format!("Failed to create file {:?} on overlay", to))?;
            system::bind_mount(&from, &to).with_context(|| {
                format!("Failed to bind-mount {:?} -> {:?} on overlay", from, to)
            })?;
        }

        // Mount /dev on overlay
        std::fs::create_dir("/tmp/worker/overlay/dev")
            .with_context(|| "Failed to create .../dev")?;
        system::bind_mount("/tmp/dev", "/tmp/worker/overlay/dev")
            .with_context(|| "Failed to mount /dev on overlay")?;

        // Change root
        std::fs::create_dir("/tmp/worker/overlay/old-root")
            .with_context(|| "Failed to create .../old-root")?;
        std::env::set_current_dir("/tmp/worker/overlay")
            .with_context(|| "Failed to chdir to new root")?;
        nix::unistd::pivot_root("/tmp/worker/overlay", "/tmp/worker/overlay/old-root")
            .with_context(|| "Failed to pivot_root")?;
        system::umount_opt("/old-root", system::MNT_DETACH)
            .with_context(|| "Failed to unmount /old-root")?;

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
        mut sandbox_config: SandboxConfig,
    ) -> Result<()> {
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

        for (input_file, input_pattern) in &files_and_patterns {
            sandbox_config
                .bound_files
                .push((input_file.into(), input_pattern.replace("%", &pre_pattern)));
        }
        self.package
            .enter(&sandbox_config)
            .with_context(|| format!("Failed to enter sandbox for build"))?;

        // Evaluate correct pattern
        let pattern: String = lisp::evaluate(
            self.config.base_rule.clone(),
            &lisp::State::new().set_var("$base".to_string(), pre_pattern.clone()),
        )?
        .to_native()?;

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
                    .with_context(|| format!("Failed to create file {:?} on overlay", new_path))?;
                system::move_mount(&old_path, &new_path).with_context(|| {
                    format!(
                        "Failed to move mount {:?} -> {:?} on overlay",
                        &old_path, new_path
                    )
                })?;
                std::fs::remove_file(&old_path).with_context(|| {
                    format!("Failed to remove old file {:?} on overlay", old_path)
                })?;
            }
        }

        // Run build process
        let build_output: String = lisp::evaluate(
            self.config.build.clone(),
            &lisp::State::new().set_var("$base".to_string(), pattern.clone()),
        )?
        .to_native()?;

        // TODO: log?
        // println!("build output: {}", build_output);

        Ok(())
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
