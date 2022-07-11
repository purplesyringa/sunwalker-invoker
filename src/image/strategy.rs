use crate::{
    errors,
    errors::{ToError, ToResult},
    image::{ids, program, sandbox},
    problem::verdict,
    system,
};
use multiprocessing::{Bind, Object};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::io::{Read, Seek, Write};
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::PathBuf;

#[derive(Clone, Object, Deserialize, Serialize)]
pub struct StrategyFactory {
    files: HashMap<String, FileType>,
    blocks: Vec<Block>,
    programs: HashMap<String, program::CachedProgram>,
    pub root: PathBuf,
}

pub struct Strategy {
    files: HashMap<String, FileType>,
    blocks: Vec<Block>,
    invocable_programs: Vec<program::InvocableProgram>,
    components: Vec<Vec<usize>>,
    writer_by_file: HashMap<String, usize>,
    written_files_by_block: Vec<Vec<String>>,
    invocation_limits: HashMap<String, verdict::InvocationLimit>,
    core: u64,
}

#[derive(Clone, Object, Deserialize, Serialize)]
struct Block {
    name: String,
    tactic: Tactic,
    bindings: HashMap<String, Binding>,
    command: String,
    argv: Vec<Pattern>,
    stdin: Option<Pattern>,
    stdout: Option<Pattern>,
    stderr: Option<Pattern>,
}

#[derive(Clone, Object, Deserialize, Serialize)]
enum Tactic {
    User,
    Testlib,
}

#[derive(Clone, Copy, Debug, Object, Deserialize, Serialize)]
enum FileType {
    Regular,
    Fifo, // like Pipe, but with a path on filesystem
    Pipe, // like Fifo, but purely a fd
}

#[derive(Clone, Object, Deserialize, Serialize)]
struct Binding {
    readable: bool,
    writable: bool,
    source: Pattern,
}

#[derive(Clone, Object, Deserialize, Serialize)]
enum Pattern {
    File(String),
    VariableText(String),
}

struct StrategyRun<'a> {
    strategy: &'a mut Strategy,
    aux: String,
    test_path: PathBuf,
    removed: bool,
}

impl StrategyFactory {
    pub async fn make<'a>(
        &'a self,
        user_program: &'a program::Program,
        invocation_limits: HashMap<String, verdict::InvocationLimit>,
        core: u64,
    ) -> Result<Strategy, errors::Error> {
        // Sanity checks
        let mut seen_block_names = HashSet::new();
        for block in self.blocks.iter() {
            if !seen_block_names.insert(block.name.clone()) {
                return Err(errors::ConfigurationFailure(format!(
                    "Several blocks have name '{}'",
                    block.name
                )));
            }

            if !invocation_limits.contains_key(&block.name) {
                return Err(errors::ConfigurationFailure(format!(
                    "Invocation limit missing for block '{}'",
                    block.name
                )));
            }

            for (_, binding) in block.bindings.iter() {
                match binding.source {
                    Pattern::File(ref filename) => match self.files.get(filename) {
                        Some(file_type) => {
                            if let FileType::Pipe = file_type {
                                return Err(errors::ConfigurationFailure(format!(
                                    "Pipe %{filename} is bound to a file; this is not allowed"
                                )));
                            }
                        }
                        None => {
                            return Err(errors::ConfigurationFailure(format!(
                                "Use of undeclared file %{filename}"
                            )));
                        }
                    },
                    Pattern::VariableText(ref text) => {
                        // Binding from the sandbox into the sandbox is not supported
                        if !text.contains('$') {
                            return Err(errors::ConfigurationFailure(
                                "A name cannot be bound to a file inside the sandbox, but only to \
                                 %* or $test*"
                                    .to_string(),
                            ));
                        }
                        // External files cannot be written to
                        if binding.writable {
                            return Err(errors::ConfigurationFailure(format!(
                                "External file {text} is written to; this is not allowed"
                            )));
                        }
                    }
                }
            }

            for arg in block.argv.iter() {
                if let Pattern::File(ref filename) = arg {
                    match self.files.get(filename) {
                        Some(file_type) => {
                            if let FileType::Pipe = file_type {
                                return Err(errors::ConfigurationFailure(format!(
                                    "Pipe %{filename} is used as an argument; this is not allowed"
                                )));
                            }
                        }
                        None => {
                            return Err(errors::ConfigurationFailure(format!(
                                "Use of undeclared file %{filename}"
                            )));
                        }
                    }
                }
            }

            // Streams must be redirected either to a bound name or to a file, and permissions have
            // to be appropriate.
            for (stream, name, writable) in [
                (&block.stdin, "stdin", false),
                (&block.stdout, "stdout", true),
                (&block.stderr, "stderr", true),
            ]
            .iter()
            {
                match stream {
                    None => (),
                    Some(Pattern::File(ref filename)) => {
                        if !self.files.contains_key(filename) {
                            return Err(errors::ConfigurationFailure(format!(
                                "Use of undeclared file %{filename}"
                            )));
                        }
                    }
                    Some(Pattern::VariableText(ref text)) => {
                        if text.contains('$') {
                            if *writable {
                                return Err(errors::ConfigurationFailure(format!(
                                    "{name} is redirected to an external file {text}; this is not \
                                     allowed"
                                )));
                            }
                        } else {
                            let binding = block.bindings.get(text).ok_or_else(|| {
                                errors::ConfigurationFailure(format!(
                                    "{name} is redirected to unbound file {text}; this is not \
                                     allowed"
                                ))
                            })?;
                            if *writable && !binding.writable {
                                return Err(errors::ConfigurationFailure(format!(
                                    "{name} is redirected to file {text}, which is bound \
                                     read-only; this is not allowed"
                                )));
                            }
                            if !writable && !binding.readable {
                                return Err(errors::ConfigurationFailure(format!(
                                    "{name} is redirected from file {text}, which is bound \
                                     write-only; this is not allowed"
                                )));
                            }
                        }
                    }
                }
            }

            // stderr of testlib must be redirected to a regular file
            if let Tactic::Testlib = block.tactic {
                match block.stderr {
                    None => {
                        return Err(errors::ConfigurationFailure(
                            "stderr of testlib must be redirected to a regular file".to_string(),
                        ))
                    }
                    Some(ref stderr) => {
                        let filename = match stderr {
                            Pattern::File(ref filename) => filename,
                            Pattern::VariableText(ref text) => match block.bindings[text].source {
                                Pattern::File(ref filename) => filename,
                                Pattern::VariableText(_) => {
                                    return Err(errors::InvokerFailure(
                                        "The impossible happened: stderr is bound to a file that \
                                         is mapped to an external file"
                                            .to_string(),
                                    ))
                                }
                            },
                        };
                        let file_type = &self.files[filename];
                        match file_type {
                            FileType::Regular => (),
                            _ => {
                                return Err(errors::ConfigurationFailure(format!(
                                    "stderr of testlib must be redirected to a regular file, not \
                                     {file_type:?}"
                                )))
                            }
                        }
                    }
                }
            }
        }

        // Figure out the correct order
        let mut readers_and_writer_by_file: HashMap<&'a str, (Vec<usize>, Option<usize>)> =
            HashMap::new();
        for (i, block) in self.blocks.iter().enumerate() {
            for (_, binding) in block.bindings.iter() {
                if let Pattern::File(ref name) = binding.source {
                    let (readers, writer) = readers_and_writer_by_file.entry(name).or_default();
                    if binding.readable {
                        readers.push(i);
                    }
                    if binding.writable {
                        if let Some(x) = *writer {
                            if x != i {
                                return Err(errors::ConfigurationFailure(format!(
                                    "File %{name} is written to by multiple blocks; this is not \
                                     allowed"
                                )));
                            }
                        }
                        *writer = Some(i);
                    }
                }
            }
            for arg in block.argv.iter() {
                if let Pattern::File(ref name) = *arg {
                    let (readers, _) = readers_and_writer_by_file.entry(name).or_default();
                    readers.push(i);
                }
            }
            if let Some(Pattern::File(ref name)) = block.stdin {
                let (readers, _) = readers_and_writer_by_file.entry(name).or_default();
                readers.push(i);
            }
            if let Some(Pattern::File(ref name)) = block.stdout {
                let (_, writer) = readers_and_writer_by_file.entry(name).or_default();
                if let Some(x) = *writer {
                    if x != i {
                        return Err(errors::ConfigurationFailure(format!(
                            "File %{name} is written to by multiple blocks; this is not allowed"
                        )));
                    }
                }
                *writer = Some(i);
            }
            if let Some(Pattern::File(ref name)) = block.stderr {
                let (_, writer) = readers_and_writer_by_file.entry(name).or_default();
                if let Some(x) = writer {
                    if *x != i {
                        return Err(errors::ConfigurationFailure(format!(
                            "File %{name} is written to by multiple blocks; this is not allowed"
                        )));
                    }
                }
                *writer = Some(i);
            }
        }

        // The writer of a file goes before (or in parallel with) all the readers of the file
        let mut necessarily_after: Vec<Vec<usize>> = vec![Vec::new(); self.blocks.len()];
        let mut necessarily_before = necessarily_after.clone();
        for (name, (readers, writer)) in readers_and_writer_by_file.iter() {
            if let Some(writer) = writer {
                // Allow writer without readers because the file will be logged anyways, which
                // counts as reading
                for reader in readers {
                    if reader != writer {
                        necessarily_after[*reader].push(*writer);
                        necessarily_before[*writer].push(*reader);
                    }
                }
            } else {
                if readers.is_empty() {
                    return Err(errors::ConfigurationFailure(format!(
                        "File %{name} is neither read from nor written to; this is not allowed"
                    )));
                } else {
                    return Err(errors::ConfigurationFailure(format!(
                        "File %{name} is read from but not written to; this is not allowed"
                    )));
                }
            }
        }

        // Find strongly connected components
        fn dfs_scc1(
            u: usize,
            used: &mut Vec<bool>,
            necessarily_after: &Vec<Vec<usize>>,
            order: &mut Vec<usize>,
        ) {
            if used[u] {
                return;
            }
            used[u] = true;
            for v in necessarily_after[u].iter() {
                dfs_scc1(*v, used, necessarily_after, order);
            }
            order.push(u);
        }
        let mut used = vec![false; self.blocks.len()];
        let mut order = Vec::new();
        for u in 0..self.blocks.len() {
            dfs_scc1(u, &mut used, &necessarily_after, &mut order);
        }
        fn dfs_scc2(
            u: usize,
            used: &mut Vec<bool>,
            component: &mut Vec<usize>,
            necessarily_before: &Vec<Vec<usize>>,
        ) {
            used[u] = true;
            component.push(u);
            for v in necessarily_before[u].iter() {
                if !used[*v] {
                    dfs_scc2(*v, used, component, necessarily_before);
                }
            }
        }
        used.fill(false);
        let mut components: Vec<Vec<usize>> = Vec::new();
        for u in order.iter().rev() {
            if !used[*u] {
                let mut component = Vec::new();
                dfs_scc2(*u, &mut used, &mut component, &necessarily_before);
                components.push(component);
            }
        }
        components.reverse();
        let mut component_of_block: Vec<usize> = vec![0; self.blocks.len()];
        for (i, component) in components.iter().enumerate() {
            for block in component.iter() {
                component_of_block[*block] = i;
            }
        }

        // Ensure lack of races
        for (name, (readers, writer)) in readers_and_writer_by_file.iter() {
            let writer = writer.unwrap();
            let writer_component = component_of_block[writer];
            match self.files[*name] {
                FileType::Regular => {
                    for reader in readers.iter() {
                        if *reader == writer {
                            continue;
                        }
                        if component_of_block[*reader] == writer_component {
                            return Err(errors::ConfigurationFailure(format!(
                                "Regular file %{name} is written to by block '{}' and read from \
                                 by block '{}', but these blocks are executed concurrently; such \
                                 races are not allowed",
                                self.blocks[writer].name, self.blocks[*reader].name
                            )));
                        }
                    }
                }
                FileType::Fifo | FileType::Pipe => {
                    if readers.is_empty() {
                        return Err(errors::ConfigurationFailure(format!(
                            "Pipe %{name} is never read from; this is not allowed, because pipes \
                             are not logged"
                        )));
                    }
                    if readers.len() > 1 {
                        return Err(errors::ConfigurationFailure(format!(
                            "Pipe %{name} is read by multiple blocks; this data race is not \
                             allowed"
                        )));
                    }
                    let reader = readers[0];
                    if component_of_block[reader] != writer_component {
                        return Err(errors::ConfigurationFailure(format!(
                            "Pipe %{name} is written to by block '{}' and read from by block \
                             '{}', but these blocks are not executed concurrently, which will \
                             lead to blocking; this is not allowed",
                            self.blocks[writer].name, self.blocks[reader].name
                        )));
                    }
                }
            }
        }

        // Create invocable instances of the program for each block. This may create more than one
        // instance of a program, but this is reasonable: two instances of the same program may be
        // run concurrently
        let mut invocable_programs: Vec<program::InvocableProgram> = Vec::new();
        for (i, block) in self.blocks.iter().enumerate() {
            let program;
            if block.command == "user" {
                program = user_program.clone();
            } else {
                program = program::Program::from_cached_program(
                    self.programs
                        .get(&block.command)
                        .ok_or_else(|| {
                            errors::ConfigurationFailure(format!(
                                "Program {} is referenced but does not exist",
                                block.command
                            ))
                        })?
                        .clone(),
                    &self.root.join("programs").join(&block.command),
                    user_program.package.image.clone(),
                )?;
            }
            invocable_programs.push(program.into_invocable(format!("block-{i}")).await?);
        }

        // Create cgroups
        for i in 0..self.blocks.len() {
            let dir = format!("/sys/fs/cgroup/sunwalker_root/cpu_{core}/block-{i}");
            std::fs::create_dir(&dir)
                .or_else(|e| {
                    if e.kind() == std::io::ErrorKind::AlreadyExists {
                        Ok(())
                    } else {
                        Err(e)
                    }
                })
                .with_context_invoker(|| format!("Unable to create {dir} directory"))?;

            // There was code that limited the CPU usage of the process via cpu.max. That turned out
            // to be a bad idea for the following reason:
            //
            // Cgroups don't notify us when the allotted time runs out, but it lets us optimize CPU
            // utilization slightly.
            //
            // The format of cpu.max is "$MAX $PERIOD", which means "at most $MAX CPU time in
            // $PERIOD". The units are microseconds, and the period is at most one second. This
            // means we can tell the scheduler to stop running the program if it has already run for
            // some period of time, but this data is reset in $PERIOD, so this is only reasonable if
            // TL is less than one second.
            //
            // But there's a much worse fact. The allotted chunk sort of survives between
            // invocations, which means that if, say, $MAX is 300ms and $PERIOD is 1s and a program
            // used up 200ms, then the next program will use the other 100ms, wait for about 700ms
            // for the limits to reset, and then continue execution. This is certainly not what we
            // want.
        }

        let mut writer_by_file = HashMap::new();
        let mut written_files_by_block = vec![Vec::new(); self.blocks.len()];
        for (name, (_, writer)) in readers_and_writer_by_file.into_iter() {
            let writer = writer.unwrap();
            writer_by_file.insert(name.to_string(), writer);
            written_files_by_block[writer].push(name.to_string());
        }

        Ok(Strategy {
            files: self.files.clone(),
            blocks: self.blocks.clone(),
            invocable_programs,
            components,
            writer_by_file,
            written_files_by_block,
            invocation_limits,
            core,
        })
    }
}

impl Strategy {
    pub async fn invoke(
        &mut self,
        build_id: String,
        test_path: PathBuf,
    ) -> Result<verdict::TestJudgementResult, errors::Error> {
        let aux = format!("/tmp/sunwalker_invoker/worker/aux/{build_id}");

        std::fs::create_dir(&aux).with_context_invoker(|| {
            format!("Failed to create directory {aux} to start running a strategy")
        })?;

        (StrategyRun {
            strategy: self,
            aux,
            test_path,
            removed: false,
        })
        .invoke()
        .await
    }
}

impl<'a> StrategyRun<'a> {
    async fn invoke(mut self) -> Result<verdict::TestJudgementResult, errors::Error> {
        // Create files on filesystem and in memory
        let mut pipes: HashMap<String, (OwnedFd, OwnedFd)> = HashMap::new();
        for (name, file_type) in self.strategy.files.iter() {
            match file_type {
                FileType::Regular => {
                    // Handled later
                }
                FileType::Fifo => {
                    let path = format!("{}/{name}", self.aux);
                    nix::unistd::mkfifo::<str>(
                        &path,
                        nix::sys::stat::Mode::from_bits_truncate(0600),
                    )
                    .with_context_invoker(|| {
                        format!(
                            "Failed to mkfifo {}/{name} to start running a strategy",
                            self.aux
                        )
                    })?;
                    std::os::unix::fs::chown(
                        &path,
                        Some(ids::EXTERNAL_USER_UID),
                        Some(ids::EXTERNAL_USER_GID),
                    )
                    .with_context_invoker(|| format!("Failed to chown {path}"))?;
                }
                FileType::Pipe => {
                    let (rx, tx) = nix::unistd::pipe()
                        .context_invoker("Failed to create a pipe to start running a strategy")?;
                    nix::sys::stat::fchmod(rx, nix::sys::stat::Mode::from_bits_truncate(0444))
                        .context_invoker("Failed to make a pipe world-readable")?;
                    nix::sys::stat::fchmod(tx, nix::sys::stat::Mode::from_bits_truncate(0222))
                        .context_invoker("Failed to make a pipe world-writable")?;
                    pipes.insert(name.to_string(), unsafe {
                        (OwnedFd::from_raw_fd(rx), OwnedFd::from_raw_fd(tx))
                    });
                }
            }
        }

        // Run programs
        let mut verdict = verdict::TestVerdict::Accepted;
        let mut invocation_stats = HashMap::new();
        let mut logs = HashMap::new();

        'comps: for component in self.strategy.components.iter() {
            let mut processes = Vec::new();
            for block_id in component.iter() {
                let block = &self.strategy.blocks[*block_id];
                let program = &self.strategy.invocable_programs[*block_id];

                // Clean up
                program.rootfs.reset().with_context_invoker(|| {
                    format!("Failed to reset rootfs for {}", block.command)
                })?;

                // Create regular files that are written by this block
                for name in self.strategy.written_files_by_block[*block_id].iter() {
                    let file_type = self.strategy.files[name];
                    if let FileType::Regular = file_type {
                        // Regular files are to be created inside the filesystem of the block that
                        // writes to it
                        let overlay = program.rootfs.overlay();
                        let path = format!("{overlay}/space/.file-{name}");
                        std::fs::write(&path, "").with_context_invoker(|| {
                            format!("Failed to touch file {path} to start running a strategy")
                        })?;
                        std::os::unix::fs::chown(
                            &path,
                            Some(ids::EXTERNAL_USER_UID),
                            Some(ids::EXTERNAL_USER_GID),
                        )
                        .with_context_invoker(|| format!("Failed to chown {path}"))?;
                    }
                }

                // Filesystem bindings
                for (filename, binding) in block.bindings.iter() {
                    let outer_path = self.resolve_outer_path(&binding.source, None)?;
                    let inner_path = format!("{}/space/{filename}", program.rootfs.overlay());
                    std::fs::write(&inner_path, "")
                        .with_context_invoker(|| format!("Failed to create {inner_path}"))?;
                    system::bind_mount_opt(
                        &outer_path,
                        &inner_path,
                        if binding.writable {
                            system::MS_RDONLY
                        } else {
                            0
                        },
                    )
                    .with_context_invoker(|| {
                        format!("Failed to bind-mount {outer_path:?} to {inner_path}")
                    })?;
                }

                // Binding via arguments
                let mut patched_argv = program.program.argv.clone();
                for (i, arg) in block.argv.iter().enumerate() {
                    if let Pattern::VariableText(ref text) = arg {
                        if !text.contains('$') {
                            patched_argv.push(text.clone());
                            continue;
                        }
                    }

                    let outer_path = self.resolve_outer_path(&arg, None)?;
                    let inner_path = format!("{}/space/.arg-{i}", program.rootfs.overlay());
                    std::fs::write(&inner_path, "")
                        .with_context_invoker(|| format!("Failed to create {inner_path}"))?;
                    system::bind_mount_opt(&outer_path, &inner_path, system::MS_RDONLY)
                        .with_context_invoker(|| {
                            format!("Failed to bind-mount {outer_path:?} to {inner_path}")
                        })?;

                    patched_argv.push(format!("/space/.arg-{i}"));
                }

                // Prepare streams
                let mut stdin: Option<std::fs::File> = None;
                let mut stdout: Option<std::fs::File> = None;
                let mut stderr: Option<std::fs::File> = None;

                for (stream_ref, name, stream, writable) in [
                    (&block.stdin, "stdin", &mut stdin, false),
                    (&block.stdout, "stdout", &mut stdout, true),
                    (&block.stderr, "stderr", &mut stderr, true),
                ]
                .into_iter()
                {
                    let outer_path: PathBuf = match stream_ref {
                        None => "/dev/null".into(),
                        Some(ref stream_ref) => {
                            if let Pattern::File(ref filename) = *stream_ref {
                                if let FileType::Pipe = self.strategy.files[filename] {
                                    let (tx, rx) = &pipes[filename];
                                    *stream = Some(
                                        (if writable { tx } else { rx })
                                            .try_clone()
                                            .context_invoker("Failed to dup(2) a file descriptor")?
                                            .into(),
                                    );
                                    continue;
                                }
                            }
                            self.resolve_outer_path(stream_ref, Some(program.rootfs.overlay()))?
                        }
                    };
                    *stream = Some(
                        std::fs::File::options()
                            .read(!writable)
                            .write(writable)
                            .open(&outer_path)
                            .with_context_invoker(|| {
                                format!("Failed to redirect {name} to {outer_path:?}")
                            })?,
                    );
                }

                processes.push(sandbox::run_isolated(
                    Box::new(
                        execute
                            .bind(patched_argv)
                            .bind(stdin.unwrap())
                            .bind(stdout.unwrap())
                            .bind(stderr.unwrap())
                            .bind(
                                self.strategy
                                    .invocation_limits
                                    .get(&block.name)
                                    .unwrap()
                                    .clone(),
                            )
                            // Open the cgroup files here because /sys/fs/cgroup is not mounted
                            // inside the sandbox and is owned by real root, not fake root
                            .bind(
                                CgroupHandle::open(
                                    format!(
                                        "/sys/fs/cgroup/sunwalker_root/cpu_{}/block-{block_id}",
                                        self.strategy.core
                                    )
                                    .as_ref(),
                                )
                                .context_invoker("Failed to open user cgroup")?,
                            ),
                    ),
                    &program.rootfs,
                ));
            }

            let mut process_results = Vec::new();
            for res in futures::future::join_all(processes.into_iter()).await {
                process_results.push(res?);
            }

            // Collect logs and stats
            for (block_id, (_test_verdict, stat)) in
                std::iter::zip(component.iter(), process_results.iter())
            {
                let block = &self.strategy.blocks[*block_id];
                let program = &self.strategy.invocable_programs[*block_id];

                invocation_stats.insert(block.name.clone(), stat.clone());

                for name in self.strategy.written_files_by_block[*block_id].iter() {
                    let file_type = self.strategy.files[name];
                    if let FileType::Regular = file_type {
                        let data = program.rootfs.read(&format!("/space/.file-{name}"))?;
                        logs.insert(name.to_string(), data);
                    }
                }
            }

            // Collect user exit codes and exit immediately on failure
            for (block_id, (test_verdict, _stat)) in
                std::iter::zip(component.iter(), process_results.iter())
            {
                let block = &self.strategy.blocks[*block_id];
                if let Tactic::User = block.tactic {
                    match *test_verdict {
                        verdict::TestVerdict::Accepted => {}
                        _ => {
                            verdict = test_verdict.clone();
                            break 'comps;
                        }
                    }
                }
            }

            // Collect testlib exit codes and exit immediately on failure
            for (block_id, (test_verdict, _stat)) in
                std::iter::zip(component.iter(), process_results.iter())
            {
                let block = &self.strategy.blocks[*block_id];
                if let Tactic::Testlib = block.tactic {
                    let program = &self.strategy.invocable_programs[*block_id];

                    let filename = match block.stderr.as_ref().unwrap() {
                        Pattern::File(ref filename) => filename,
                        Pattern::VariableText(ref text) => {
                            // It was asserted above that 'text' is an internal path
                            match block.bindings[text].source {
                                Pattern::File(ref filename) => filename,
                                Pattern::VariableText(_) => {
                                    return Err(errors::InvokerFailure(
                                        "The impossible happened: stderr is bound to a file that \
                                         is mapped to an external file"
                                            .to_string(),
                                    ))
                                }
                            }
                        }
                    };
                    let testlib_stderr =
                        program.rootfs.read(&format!("/space/.file-{filename}"))?;

                    let exit_status = match *test_verdict {
                        verdict::TestVerdict::Accepted => verdict::ExitStatus::ExitCode(0),
                        verdict::TestVerdict::RuntimeError(exit_status) => exit_status,
                        _ => {
                            verdict = verdict::TestVerdict::Bug(format!(
                                "Testlib task '{}' failed with verdict {}",
                                block.name,
                                test_verdict.to_short_string(),
                            ));
                            break 'comps;
                        }
                    };

                    let current_verdict =
                        verdict::TestVerdict::from_testlib(exit_status, &testlib_stderr);

                    match current_verdict {
                        verdict::TestVerdict::Accepted => (),
                        _ => {
                            verdict = current_verdict;
                            break 'comps;
                        }
                    }
                }
            }
        }

        // Cleanup
        self.removed = true;
        std::fs::remove_dir_all(&self.aux).with_context_invoker(|| {
            format!(
                "Failed to remove {} recursively while finishing a strategy",
                self.aux,
            )
        })?;

        Ok(verdict::TestJudgementResult {
            verdict,
            logs,
            invocation_stats,
        })
    }

    fn resolve_outer_path(
        &self,
        pat: &Pattern,
        root: Option<String>,
    ) -> Result<PathBuf, errors::Error> {
        match pat {
            Pattern::File(ref name) => {
                if let FileType::Regular = self.strategy.files[name] {
                    Ok(format!(
                        "{}/space/.file-{name}",
                        self.strategy.invocable_programs[self.strategy.writer_by_file[name]]
                            .rootfs
                            .overlay()
                    )
                    .into())
                } else {
                    Ok(format!("{}/{name}", self.aux).into())
                }
            }
            Pattern::VariableText(ref text) => {
                if text.contains('$') {
                    if !text.starts_with("$test") || text.matches('$').count() > 1 {
                        return Err(errors::ConfigurationFailure(format!(
                            "Path {text} is invalid: it must start with $test"
                        )));
                    }
                    let mut path = self.test_path.as_os_str().to_owned();
                    path.push(&text[5..]);
                    Ok(path.into())
                } else {
                    Ok(format!("{}/space/{text}", root.unwrap()).into())
                }
            }
        }
    }
}

impl Drop for StrategyRun<'_> {
    fn drop(&mut self) {
        if !self.removed {
            if let Err(e) = std::fs::remove_dir_all(&self.aux) {
                println!("Failed to clean up {} in drop(): {e:?}", self.aux);
            }
        }
    }
}

#[derive(Clone, Copy)]
struct CgroupCpuStat {
    user: std::time::Duration,
    system: std::time::Duration,
    total: std::time::Duration,
}

impl std::ops::Sub for CgroupCpuStat {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self {
        Self {
            user: self.user - rhs.user,
            system: self.system - rhs.system,
            total: self.total - rhs.total,
        }
    }
}

#[derive(Object)]
struct CgroupHandle {
    cgroup_procs: std::fs::File,
    cpu_stat: std::fs::File,
}

impl CgroupHandle {
    fn open(path: &std::path::Path) -> Result<Self, errors::Error> {
        Ok(CgroupHandle {
            cgroup_procs: std::fs::File::options()
                .write(true)
                .open(path.join("cgroup.procs"))
                .context_invoker("Failed to open cgroup.procs")?,
            cpu_stat: std::fs::File::open(path.join("cpu.stat"))
                .context_invoker("Failed to open cpu.stat")?,
        })
    }

    fn add_process(&mut self, pid: libc::pid_t) -> Result<(), errors::Error> {
        self.cgroup_procs
            .write(format!("{pid}\n").as_bytes())
            .context_invoker("Failed to write to cgroup.procs")?;
        Ok(())
    }

    fn cpu_stat(&mut self) -> Result<CgroupCpuStat, errors::Error> {
        self.cpu_stat
            .rewind()
            .context_invoker("Failed to rewind cpu.stat")?;

        let mut buf = String::new();
        self.cpu_stat
            .read_to_string(&mut buf)
            .context_invoker("Failed to read cpu.stat")?;

        let mut stat = CgroupCpuStat {
            user: std::time::Duration::ZERO,
            system: std::time::Duration::ZERO,
            total: std::time::Duration::ZERO,
        };

        for line in buf.lines() {
            let target;
            if line.starts_with("user_usec ") {
                target = &mut stat.user;
            } else if line.starts_with("system_usec ") {
                target = &mut stat.system;
            } else {
                continue;
            }

            let mut it = line.split_ascii_whitespace();
            it.next();
            *target = std::time::Duration::from_micros(
                it.next()
                    .context_invoker("Invalid cpu.stat format")?
                    .parse()
                    .context_invoker("Invalid cpu.stat format")?,
            );
        }

        stat.total = stat.user + stat.system;

        Ok(stat)
    }
}

#[multiprocessing::entrypoint]
fn execute(
    argv: Vec<String>,
    stdin: std::fs::File,
    stdout: std::fs::File,
    stderr: std::fs::File,
    invocation_limit: verdict::InvocationLimit,
    mut cgroup: CgroupHandle,
) -> Result<(verdict::TestVerdict, verdict::InvocationStat), errors::Error> {
    // Start process
    let (mut ours, theirs) =
        multiprocessing::duplex().context_invoker("Failed to create a pipe")?;

    let proc = executor_worker
        .spawn(
            argv,
            stdin,
            stdout,
            stderr,
            theirs,
            invocation_limit.cpu_time,
        )
        .context_invoker("Failed to spawn the child")?;
    let pid = proc.id();

    // Acquire pidfd for the process. This is safe because the process hasn't been awaited yet. We
    // prefer younger pidfd to older signalfd because our process is PID 1 and therefore reaps all
    // orphan processes, so SIGCHLD may fire for a process that was not our direct descendant.
    let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as RawFd;
    if pidfd == -1 {
        return Err(std::io::Error::last_os_error())
            .context_invoker("Failed to open pidfd for child process");
    }

    // Apply cgroup limits
    cgroup
        .add_process(pid)
        .context_invoker("Failed to move the child to user cgroup")?;

    // Acquire previous cgroup stats. We reuse cgroups across tests and even across submissions, so
    // we can't assume the stats are at zero at this moment.
    let cpu_stat_before = cgroup.cpu_stat()?;

    // Measure time. It would be slightly before execve, but it should not be a big problem
    let start = std::time::Instant::now();

    // Tell the child it's alright to start
    if let Err(_) = ours.send(&()) {
        // This most likely indicates that the child has terminated before having a chance to wait
        // on the pipe, i.e. a preparation failure
        return Err(ours
            .recv()
            .context_invoker("Failed to read an error from the child")?
            .context_invoker("The child terminated preemptively but did not report any error")?);
    }

    // The child will either report an error during execve, or nothing if execve succeeded and the
    // pipe was closed automatically because it's CLOEXEC.
    if let Some(e) = ours
        .recv()
        .context_invoker("Failed to read an error from the child")?
    {
        return Err(e.context_invoker("Child returned an error"));
    }

    // Create a timerfd for tracking real time limit. It would perhaps be more correct to account
    // for the lapse of time between starting the process and creating the timerfd, but the
    // difference is negligible.
    use nix::sys::timerfd::*;
    let timer = TimerFd::new(ClockId::CLOCK_MONOTONIC, TimerFlags::empty())
        .context_invoker("Failed to create timerfd")?;
    timer
        .set(
            Expiration::OneShot(nix::sys::time::TimeSpec::from_duration(
                invocation_limit.real_time,
            )),
            TimerSetTimeFlags::empty(),
        )
        .context_invoker("Failed to configure timerfd")?;

    // Listen for events
    use nix::sys::epoll::*;
    let epollfd = epoll_create().context_invoker("Failed to create epollfd")?;

    epoll_ctl(
        epollfd,
        EpollOp::EpollCtlAdd,
        pidfd,
        &mut EpollEvent::new(EpollFlags::EPOLLIN, 0),
    )
    .context_invoker("Failed to configure epoll")?;

    epoll_ctl(
        epollfd,
        EpollOp::EpollCtlAdd,
        timer.as_raw_fd(),
        &mut EpollEvent::new(EpollFlags::EPOLLIN, 1),
    )
    .context_invoker("Failed to configure epoll")?;

    let mut events = [EpollEvent::empty()];

    // The connection between real time and cpu time is complicated. On the one hand, a process can
    // sleep, which does not count towards cpu time, so it can be as low as it gets. Secondly,
    // multithreaded applications can use several cores (TODO: add opt-in support for that), and
    // that means cpu time may exceed real time. The inequality seems to be
    //     0 <= cpu_time <= real_time * n_cores,
    // so a process cannot exceed its cpu time limit during the first
    //     cpu_time_limit / n_cores
    // seconds. This gives us a better way to handle TLE than by polling the stats every few
    // milliseconds. Instead, the algorithm is roughly (other limits notwithstanding):
    //     while the process has not terminated and limits are not exceeded {
    //         let guaranteed_cpu_time_left = how much more cpu time the process can spend without
    //             exceeding the limit;
    //         let guaranteed_real_time_left = max_cpu_time_left / n_cores;
    //         sleep(guaranteed_real_time_left);
    //     }

    let mut real_time_timeout = false;
    loop {
        let cpu_stat = cgroup.cpu_stat()? - cpu_stat_before;

        if cpu_stat.total > invocation_limit.cpu_time {
            // Sending a signal to a zombie process is fine, so races are not a problem
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), nix::sys::signal::SIGKILL)
                .context_invoker("Failed to kill the process")?;
            break;
        }

        let timeout = invocation_limit.cpu_time - cpu_stat.total;
        let n_events = epoll_wait(
            epollfd,
            &mut events,
            // Switching context takes time, some other operations take time too, etc., so less cpu
            // time is usually used than permitted. We also don't really want to interrupt the
            // process. We need to set a low limit on the timeout as well.
            //
            // In practice, adding 50ms seems like a good solution. This is not too big a number to
            // slow the judgment, not too small to steal resources from the solution in what is
            // effectively a spin lock, and allows SIGPROF to fire just at the right moment under
            // normal circumstances.
            (timeout.as_millis().min(1000000) as isize) + 50,
        )
        .context_invoker("epoll_wait failed")?;

        match n_events {
            0 => {
                // End of allotted real time chunk, will check if the limit was exceeded on the next
                // iteration of the loop
            }
            1 => {
                match events[0].data() {
                    0 => {
                        // pidfd fired -- the process has terminated
                    }
                    1 => {
                        // timerfd fired -- time out
                        real_time_timeout = true;
                        // Sending a signal to a zombie process is fine, so races are not a problem
                        nix::sys::signal::kill(
                            nix::unistd::Pid::from_raw(pid),
                            nix::sys::signal::SIGKILL,
                        )
                        .context_invoker("Failed to kill the process")?;
                    }
                    _ => {
                        return Err(errors::InvokerFailure(
                            "Invalid epollfd data returned".to_string(),
                        ));
                    }
                }
                break;
            }
            _ => {
                return Err(std::io::Error::last_os_error())
                    .with_context_invoker(|| format!("epoll_wait returned {n_events}"));
            }
        }
    }

    // Collect real time statistics
    let real_time = start.elapsed();

    // Collect current stats; we will later compute the difference between the current stats and the
    // previous stats
    let cpu_stat = cgroup.cpu_stat()? - cpu_stat_before;

    let wait_status = nix::sys::wait::waitpid(nix::unistd::Pid::from_raw(pid), None)
        .context_invoker("Failed to waitpid for process")?;

    let cpu_time_timeout = match wait_status {
        nix::sys::wait::WaitStatus::Signaled(_, nix::sys::signal::Signal::SIGPROF, _) => true,
        _ => false,
    };

    // Into verdict
    let test_verdict;
    if cpu_time_timeout || cpu_stat.total > invocation_limit.cpu_time {
        test_verdict = verdict::TestVerdict::TimeLimitExceeded;
    } else if real_time_timeout || real_time > invocation_limit.real_time {
        test_verdict = verdict::TestVerdict::IdlenessLimitExceeded;
    } else {
        match wait_status {
            nix::sys::wait::WaitStatus::Exited(_, exit_code) => {
                if exit_code == 0 {
                    test_verdict = verdict::TestVerdict::Accepted;
                } else {
                    test_verdict = verdict::TestVerdict::RuntimeError(
                        verdict::ExitStatus::ExitCode(exit_code as u8),
                    );
                }
            }
            nix::sys::wait::WaitStatus::Signaled(_, signal, _) => {
                test_verdict = verdict::TestVerdict::RuntimeError(verdict::ExitStatus::Signal(
                    signal as i32 as u8,
                ))
            }
            _ => {
                return Err(errors::InvokerFailure(format!(
                    "waitpid returned unexpected status: {wait_status:?}"
                )));
            }
        }
    }

    Ok((
        test_verdict,
        verdict::InvocationStat {
            real_time,
            cpu_time: cpu_stat.total,
            user_time: cpu_stat.user,
            sys_time: cpu_stat.system,
            memory: 0, // TODO
        },
    ))
}

#[multiprocessing::entrypoint]
fn executor_worker(
    argv: Vec<String>,
    stdin: std::fs::File,
    stdout: std::fs::File,
    stderr: std::fs::File,
    mut pipe: multiprocessing::Duplex<errors::Error, ()>,
    cpu_time_limit: std::time::Duration,
) {
    if let Err(e) = try {
        sandbox::drop_privileges().context_invoker("Failed to drop privileges")?;

        std::env::set_current_dir("/space").context_invoker("Failed to chdir to /space")?;

        nix::unistd::dup2(stdin.as_raw_fd(), nix::libc::STDIN_FILENO)
            .context_invoker("dup2 for stdin failed")?;
        nix::unistd::dup2(stdout.as_raw_fd(), nix::libc::STDOUT_FILENO)
            .context_invoker("dup2 for stdout failed")?;
        nix::unistd::dup2(stderr.as_raw_fd(), nix::libc::STDERR_FILENO)
            .context_invoker("dup2 for stderr failed")?;

        let mut args = Vec::with_capacity(argv.len());
        for arg in argv {
            args.push(
                CString::new(arg.into_bytes())
                    .context_invoker("Argument contains null character")?,
            );
        }

        pipe.recv()
            .context_invoker("Failed to await confirmation from master process")?
            .context_invoker("No confirmation from master process")?;

        // Fine to start the application now. We don't need to reset signals because we didn't
        // configure them inside executor_worker()

        // An additional optimization for finer handling of cpu time limit. An ITIMER_PROF timer can
        // emit a signal when the given limit is exceeded and is not reset upon execve. This only
        // applies to a single process, not a cgroup, and can be overwritten by the user program,
        // but this feature is not mission-critical. It merely saves us a few precious milliseconds
        // due to the (somewhat artificially deliberate) inefficiency of polling.
        let timer = libc::itimerval {
            it_interval: libc::timeval {
                tv_sec: 0,
                tv_usec: 0,
            },
            it_value: libc::timeval {
                tv_sec: cpu_time_limit.as_secs() as i64,
                tv_usec: cpu_time_limit.subsec_micros() as i64,
            },
        };
        if unsafe {
            libc::syscall(
                libc::SYS_setitimer,
                libc::ITIMER_PROF,
                &timer as *const libc::itimerval,
                std::ptr::null_mut::<libc::itimerval>(),
            )
        } == -1
        {
            Err(std::io::Error::last_os_error()).context_invoker("Failed to set interval timer")?;
        }

        // Try block wraps return value in Ok(...)
        nix::unistd::execv(&args[0], &args).context_invoker("execve failed")?;
    } {
        pipe.send(&e).expect("Failed to report error to parent");
    }
}
