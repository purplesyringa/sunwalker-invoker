use crate::{
    errors,
    errors::ToResult,
    image::{program, sandbox},
    problem::verdict,
    system,
};
use multiprocessing::{Bind, Object};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::os::unix::io::{FromRawFd, OwnedFd};
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
}

#[derive(Clone, Object, Deserialize, Serialize)]
struct Block {
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
    ) -> Result<Strategy, errors::Error> {
        // Sanity checks
        for block in self.blocks.iter() {
            for (_, binding) in block.bindings.iter() {
                match binding.source {
                    Pattern::File(ref filename) => match self.files.get(filename) {
                        Some(file_type) => {
                            if let FileType::Pipe = file_type {
                                return Err(errors::ConfigurationFailure(format!(
                                    "Pipe %{} is bound to a file; this is not allowed",
                                    filename
                                )));
                            }
                        }
                        None => {
                            return Err(errors::ConfigurationFailure(format!(
                                "Use of undeclared file %{}",
                                filename
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
                                "External file {} is written to; this is not allowed",
                                text
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
                                    "Pipe %{} is used as an argument; this is not allowed",
                                    filename
                                )));
                            }
                        }
                        None => {
                            return Err(errors::ConfigurationFailure(format!(
                                "Use of undeclared file %{}",
                                filename
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
                                "Use of undeclared file %{}",
                                filename
                            )));
                        }
                    }
                    Some(Pattern::VariableText(ref text)) => {
                        if text.contains('$') {
                            if *writable {
                                return Err(errors::ConfigurationFailure(format!(
                                    "{} is redirected to an external file {}; this is not allowed",
                                    name, text
                                )));
                            }
                        } else {
                            let binding = block.bindings.get(text).ok_or_else(|| {
                                errors::ConfigurationFailure(format!(
                                    "{} is redirected to unbound file {}; this is not allowed",
                                    name, text
                                ))
                            })?;
                            if *writable && !binding.writable {
                                return Err(errors::ConfigurationFailure(format!(
                                    "{} is redirected to file {}, which is bound read-only; this \
                                     is not allowed",
                                    name, text
                                )));
                            }
                            if !writable && !binding.readable {
                                return Err(errors::ConfigurationFailure(format!(
                                    "{} is redirected from file {}, which is bound write-only; \
                                     this is not allowed",
                                    name, text
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
                                     {:?}",
                                    file_type
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
                                    "File %{} is written to by multiple blocks; this is not \
                                     allowed",
                                    name
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
                            "File %{} is written to by multiple blocks; this is not allowed",
                            name
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
                            "File %{} is written to by multiple blocks; this is not allowed",
                            name
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
                        "File %{} is neither read from nor written to; this is not allowed",
                        name
                    )));
                } else {
                    return Err(errors::ConfigurationFailure(format!(
                        "File %{} is read from but not written to; this is not allowed",
                        name
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
                                "Regular file %{} is written to by block #{} and read from by \
                                 block #{}, but these blocks are executed concurrently; such \
                                 races are not allowed",
                                name, writer, reader
                            )));
                        }
                    }
                }
                FileType::Fifo | FileType::Pipe => {
                    if readers.is_empty() {
                        return Err(errors::ConfigurationFailure(format!(
                            "Pipe %{} is never read from; this is not allowed, because pipes are \
                             not logged",
                            name,
                        )));
                    }
                    if readers.len() > 1 {
                        return Err(errors::ConfigurationFailure(format!(
                            "Pipe %{} is read by multiple blocks; this data race is not allowed",
                            name,
                        )));
                    }
                    let reader = readers[0];
                    if component_of_block[reader] != writer_component {
                        return Err(errors::ConfigurationFailure(format!(
                            "Pipe %{} is written to by block #{} and read from by block #{}, but \
                             these blocks are not executed concurrently, which will lead to \
                             blocking; this is not allowed",
                            name, writer, reader
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
            invocable_programs.push(program.into_invocable(format!("block-{}", i)).await?);
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
        })
    }
}

impl Strategy {
    pub async fn invoke(
        &mut self,
        build_id: String,
        test_path: PathBuf,
    ) -> Result<verdict::TestJudgementResult, errors::Error> {
        let aux = format!("/tmp/sunwalker_invoker/worker/aux/{}", build_id);

        std::fs::create_dir(&aux).with_context_invoker(|| {
            format!(
                "Failed to create directory {} to start running a strategy",
                aux,
            )
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
                    nix::unistd::mkfifo::<str>(
                        format!("{}/{}", self.aux, name).as_ref(),
                        nix::sys::stat::Mode::from_bits_truncate(0666),
                    )
                    .with_context_invoker(|| {
                        format!(
                            "Failed to mkfifo {}/{} to start running a strategy",
                            self.aux, name,
                        )
                    })?;
                }
                FileType::Pipe => {
                    let (rx, tx) = nix::unistd::pipe()
                        .context_invoker("Failed to create a pipe to start running a strategy")?;
                    pipes.insert(name.to_string(), unsafe {
                        (OwnedFd::from_raw_fd(rx), OwnedFd::from_raw_fd(tx))
                    });
                }
            }
        }

        // Run programs
        let mut verdict = verdict::TestVerdict::Accepted;

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
                        std::fs::write(format!("{}/space/.file-{}", overlay, name), b"")
                            .with_context_invoker(|| {
                                format!(
                                    "Failed to touch file {}/.file-{} to start running a strategy",
                                    overlay, name,
                                )
                            })?;
                    }
                }

                // Filesystem bindings
                for (filename, binding) in block.bindings.iter() {
                    let outer_path = self.resolve_outer_path(&binding.source, None)?;
                    let inner_path = format!("{}/space/{}", program.rootfs.overlay(), filename);
                    std::fs::write(&inner_path, "")
                        .with_context_invoker(|| format!("Failed to create {}", inner_path))?;
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
                        format!("Failed to bind-mount {:?} to {}", outer_path, inner_path,)
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
                    let inner_path = format!("{}/space/.arg-{}", program.rootfs.overlay(), i);
                    std::fs::write(&inner_path, "")
                        .with_context_invoker(|| format!("Failed to create {}", inner_path))?;
                    system::bind_mount_opt(&outer_path, &inner_path, system::MS_RDONLY)
                        .with_context_invoker(|| {
                            format!("Failed to bind-mount {:?} to {}", outer_path, inner_path,)
                        })?;

                    patched_argv.push(format!("/space/.arg-{}", i));
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
                                format!("Failed to redirect {} to {:?}", name, outer_path,)
                            })?,
                    );
                }

                processes.push(sandbox::run_isolated(
                    Box::new(
                        execute
                            .bind(patched_argv)
                            .bind(stdin.unwrap())
                            .bind(stdout.unwrap())
                            .bind(stderr.unwrap()),
                    ),
                    &program.rootfs,
                    &program.namespace,
                ));
            }

            let mut process_results = Vec::new();
            for res in futures::future::join_all(processes.into_iter()).await {
                process_results.push(res?);
            }

            // Collect user exit codes
            for (block_id, result) in std::iter::zip(component.iter(), process_results.iter()) {
                let block = &self.strategy.blocks[*block_id];
                if let Tactic::User = block.tactic {
                    if *result != verdict::ExitStatus::ExitCode(0) {
                        verdict = verdict::TestVerdict::RuntimeError(*result);
                        break 'comps;
                    }
                }
            }

            // Collect testlib exit codes
            for (block_id, result) in std::iter::zip(component.iter(), process_results.iter()) {
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
                        program.rootfs.read(&format!("/space/.file-{}", filename))?;

                    let current_verdict =
                        verdict::TestVerdict::from_testlib(*result, &testlib_stderr);
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

        // Load logs
        let mut logs: HashMap<String, Vec<u8>> = HashMap::new();
        for (filename, file_type) in self.strategy.files.iter() {
            if let FileType::Regular = file_type {
                let block_id = self.strategy.writer_by_file[filename];
                let program = &self.strategy.invocable_programs[block_id];
                let data = program.rootfs.read(&format!("/space/.file-{}", filename))?;
                logs.insert(filename.to_string(), data);
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
            real_time: std::default::Default::default(), // TODO
            user_time: std::default::Default::default(),
            sys_time: std::default::Default::default(),
            memory_used: 0,
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
                        "{}/space/.file-{}",
                        self.strategy.invocable_programs[self.strategy.writer_by_file[name]]
                            .rootfs
                            .overlay(),
                        name
                    )
                    .into())
                } else {
                    Ok(format!("{}/{}", self.aux, name).into())
                }
            }
            Pattern::VariableText(ref text) => {
                if text.contains('$') {
                    if !text.starts_with("$test") || text.matches('$').count() > 1 {
                        return Err(errors::ConfigurationFailure(format!(
                            "Path {} is invalid: it must start with $test",
                            text
                        )));
                    }
                    let mut path = self.test_path.as_os_str().to_owned();
                    path.push(&text[5..]);
                    Ok(path.into())
                } else {
                    Ok(format!("{}/space/{}", root.unwrap(), text).into())
                }
            }
        }
    }
}

impl Drop for StrategyRun<'_> {
    fn drop(&mut self) {
        if !self.removed {
            std::fs::remove_dir_all(&self.aux);
        }
    }
}

#[multiprocessing::entrypoint]
fn execute(
    argv: Vec<String>,
    stdin: std::fs::File,
    stdout: std::fs::File,
    stderr: std::fs::File,
) -> Result<verdict::ExitStatus, errors::Error> {
    std::env::set_current_dir("/space").context_invoker("Failed to chdir to /space")?;

    let exit_status = std::process::Command::new(&argv[0])
        .args(&argv[1..])
        .stdin(stdin)
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
        .with_context_invoker(|| format!("Failed to spawn {:?}", argv))?
        .wait()
        .with_context_invoker(|| format!("Failed to get exit code of {:?}", argv))?;

    Ok(exit_status.into())
}
