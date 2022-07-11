use crate::{
    cgroups, config, errors, errors::ToResult, image, init, message, problem, submission, system,
};
use anyhow::Context;
use futures::stream::{SplitSink, SplitStream};
use futures_util::SinkExt;
use futures_util::StreamExt;
use libc::CLONE_NEWNS;
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::{atomic, Arc};
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio_tungstenite::tungstenite;

pub struct Communicator {
    conductor_read: Mutex<
        SplitStream<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
    >,
    conductor_write: Mutex<
        SplitSink<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            tungstenite::Message,
        >,
    >,
    next_request_id: atomic::AtomicU64,
    requests: Mutex<HashMap<u64, oneshot::Sender<Result<Vec<u8>, errors::Error>>>>,
}

pub struct Client {
    config: config::Config,
    submissions: RwLock<HashMap<String, Arc<submission::Submission>>>,
    problem_store: problem::store::ProblemStore,
    mounted_image: Arc<image::image::Image>,
    ephemeral_disk_space: u64,
    communicator: Arc<Communicator>,
}

impl Communicator {
    async fn send_to_conductor(&self, message: message::i2c::Message) -> Result<(), errors::Error> {
        self.conductor_write
            .lock()
            .await
            .send(tungstenite::Message::Binary(
                rmp_serde::to_vec(&message).map_err(|e| {
                    errors::CommunicationError(format!(
                        "Failed to serialize a message to conductor: {e:?}"
                    ))
                })?,
            ))
            .await
            .map_err(|e| {
                errors::CommunicationError(format!(
                    "Failed to send a message to conductor via websocket: {e:?}"
                ))
            })?;

        Ok(())
    }

    async fn request_file(&self, hash: &str) -> Result<Vec<u8>, errors::Error> {
        let request_id = self.next_request_id.fetch_add(1, atomic::Ordering::Relaxed);

        let (tx, rx) = oneshot::channel();
        self.requests.lock().await.insert(request_id, tx);

        self.send_to_conductor(message::i2c::Message::RequestFile(
            message::i2c::RequestFile {
                request_id,
                hash: hash.to_string(),
            },
        ))
        .await?;

        rx.await
            .context_invoker("Did not receive response to request of file")?
    }

    pub async fn download_archive(
        &self,
        topic: &str,
        target_path: &Path,
    ) -> Result<(), errors::Error> {
        std::fs::remove_dir_all(target_path);
        std::fs::create_dir_all(target_path)
            .context_invoker("Failed to create target directory")?;

        let manifest = self
            .request_file(&format!("manifest/{topic}"))
            .await
            .context_invoker("Failed to load manifest")?;

        let manifest = std::str::from_utf8(&manifest).map_err(|e| {
            errors::ConfigurationFailure(format!("Invalid manifest for topic {topic}: {e:?}"))
        })?;

        for mut line in manifest.lines() {
            if line.ends_with('/') {
                // Directory
                let dir_path = target_path.join(&line);
                std::fs::create_dir(&dir_path)
                    .with_context_invoker(|| format!("Failed to create {dir_path:?}"))?;
            } else {
                // File
                let mut executable = false;
                if line.starts_with("+x ") {
                    executable = true;
                    line = &line[3..];
                }

                // TODO: deduplication
                let (hash, file) = line
                    .split_once(' ')
                    .context_invoker("Invalid manifest: invalid line format")?;

                // TODO: stream directly to file without loading to RAM
                let data = self
                    .request_file(hash)
                    .await
                    .with_context_invoker(|| format!("Failed to download file {file}"))?;

                let file_path = target_path.join(&file);
                std::fs::write(&file_path, data)
                    .with_context_invoker(|| format!("Failed to write to {file_path:?}"))?;

                if executable {
                    let mut permissions = file_path
                        .metadata()
                        .with_context_invoker(|| {
                            format!("Failed to get metadata of {file_path:?}")
                        })?
                        .permissions();

                    // Whoever can read can also execute
                    permissions.set_mode(permissions.mode() | ((permissions.mode() & 0o444) >> 2));

                    std::fs::set_permissions(&file_path, permissions).with_context_invoker(
                        || format!("Failed to make {file_path:?} executable"),
                    )?
                }
            }
        }

        let ready_path = target_path.join(".ready");
        std::fs::write(&ready_path, b"")
            .with_context_invoker(|| format!("Failed to write to {ready_path:?}"))?;

        Ok(())
    }
}

pub fn client_main(cli_args: init::CLIArgs) -> anyhow::Result<()> {
    // Entering the sandbox must be done outside tokio runtime, because otherwise some threads are
    // not sandboxed. See the comments in src/worker.rs for more information.
    enter_sandbox()?;
    client_main_async(cli_args)
}

#[tokio::main]
async fn client_main_async(cli_args: init::CLIArgs) -> anyhow::Result<()> {
    let config = std::fs::read_to_string(&cli_args.config)
        .with_context(|| format!("Failed to read config from {}", cli_args.config))?;
    let config: config::Config = toml::from_str(&config).with_context(|| "Config is invalid")?;

    let image_cfg = std::fs::read_to_string(&config.image.config).with_context(|| {
        format!(
            "Failed to read file image.cfg from path {} (this path is from field image.config of \
             the configuration file)",
            config.image.config
        )
    })?;
    let image_cfg = image::config::Config::load(&image_cfg).with_context(|| {
        format!(
            "Failed to load image.cfg as a script from path {} (this path is from field \
             image.config of the configuration file)",
            config.image.config
        )
    })?;

    let mut mnt = image::mount::ImageMounter::new();
    let mounted_image = Arc::new(mnt.mount(&config.image.path, image_cfg).with_context(|| {
        format!(
            "Failed to mount image.sfs from path {} (this path is from field image.path of the \
             configuration file)",
            config.image.path
        )
    })?);

    cgroups::isolate_cores(&config.environment.cpu_cores).with_context(|| {
        format!(
            "Failed to isolate CPU cores {:?} (this list is from field environment.cpu_cores of \
             the configuration file)",
            config.environment.cpu_cores
        )
    })?;

    for core in &config.environment.cpu_cores {
        cgroups::create_core_cpuset(*core)
            .with_context(|| format!("Failed to create cpuset for core {core}"))?;
    }

    let (conductor_ws, _) = tokio_tungstenite::connect_async(&config.conductor.address)
        .await
        .with_context(|| {
            format!(
                "Failed to connect to the conductor via a websocket at {:?} (this address is from \
                 field conductor.address of the configuration file)",
                config.conductor.address
            )
        })?;

    let (conductor_write, conductor_read) = conductor_ws.split();

    let communicator = Arc::new(Communicator {
        conductor_write: Mutex::new(conductor_write),
        conductor_read: Mutex::new(conductor_read),
        next_request_id: atomic::AtomicU64::new(0),
        requests: Mutex::new(HashMap::new()),
    });

    let problem_store = problem::store::ProblemStore::new(
        std::path::PathBuf::from(&config.cache.problems),
        communicator.clone(),
    )
    .with_context(|| {
        format!(
            "Failed to create problem store with cache at {} (this path is from field \
             cache.problems of the configuration file)",
            config.cache.problems
        )
    })?;

    let ephemeral_disk_space: u64 = config
        .environment
        .ephemeral_disk_space
        .clone()
        .try_into()
        .with_context(|| "Failed to parse environment.ephemeral_disk_space as size")?;

    println!(
        "Connected to the conductor at address {:?}",
        config.conductor.address
    );

    let client = Arc::new(Client {
        config,
        submissions: RwLock::new(HashMap::new()),
        problem_store,
        mounted_image,
        ephemeral_disk_space,
        communicator,
    });

    // Handshake
    client
        .communicator
        .send_to_conductor(message::i2c::Message::Handshake(message::i2c::Handshake {
            invoker_name: client.config.invoker.name.clone(),
        }))
        .await?;

    // Initial mode
    client
        .communicator
        .send_to_conductor(message::i2c::Message::UpdateMode(
            message::i2c::UpdateMode {
                added_cores: client.config.environment.cpu_cores.clone(),
                removed_cores: Vec::new(),
                designated_ram: 0,
            },
        ))
        .await?;

    while let Some(message) = client.communicator.conductor_read.lock().await.next().await {
        let message = message.with_context(|| "Failed to read message from the conductor")?;
        match message {
            tungstenite::Message::Close(_) => break,
            tungstenite::Message::Binary(buf) => {
                let message: message::c2i::Message = rmp_serde::from_slice(&buf)
                    .with_context(|| "Failed to parse buffer as msgpack format")?;
                handle_message(message, client.clone()).await?;
            }
            tungstenite::Message::Ping(_) => (),
            _ => println!(
                "Message of unknown type received from the conductor: {:?}",
                message
            ),
        };
    }

    Ok(())
}

async fn handle_message(
    message: message::c2i::Message,
    client: Arc<Client>,
) -> Result<(), errors::Error> {
    use message::c2i::*;

    println!("{:?}", message);

    match message {
        Message::AddSubmission(message) => add_submission(message, client).await?,
        Message::PushToJudgementQueue(message) => push_to_judgment_queue(message, &client).await?,
        Message::CancelJudgementOnTests(message) => {
            cancel_judgement_on_tests(message, client).await?
        }
        Message::FinalizeSubmission(message) => finalize_submission(message, client).await?,
        Message::SupplyFile(message) => supply_file(message, client).await?,
    }

    Ok(())
}

async fn add_submission(
    message: message::c2i::AddSubmission,
    client: Arc<Client>,
) -> Result<(), errors::Error> {
    tokio::spawn(async move {
        let client1 = client.clone();
        let submission_id = message.submission_id.clone();

        let compilation_result = (async move {
            if !client1
                .config
                .environment
                .cpu_cores
                .contains(&message.compilation_core)
            {
                return Err(errors::ConductorFailure(format!(
                    "Core {} is not dedicated to the invoker and cannot be scheduled for compilation of a \
                     new submission",
                    message.compilation_core
                )));
            }

            if !client1.mounted_image.has_language(&message.language) {
                return Err(errors::ConductorFailure(format!(
                    "Language {} is not available",
                    message.language
                )));
            }

            let problem = client1
                .problem_store
                .load_revision(message.problem_id, message.revision_id)
                .await?;

            let mut submission = submission::Submission::new(
                message.submission_id.clone(),
                problem,
                image::image::Image::get_language(
                    client1.mounted_image.clone(),
                    message.language.clone(),
                )?,
                message.invocation_limits,
            )?;

            for (name, content) in message.files.into_iter() {
                submission.add_source_file(&name, &content)?;
            }

            let submission = Arc::new(submission);

            {
                let mut submissions = client1.submissions.write().await;
                submissions
                    .try_insert(message.submission_id.clone(), submission.clone())
                    .map_err(|_| {
                        errors::ConductorFailure(format!(
                            "A submission with ID {} cannot be added because it is already in the queue",
                            message.submission_id
                        ))
                    })?;
            }

            submission.compile_on_core(message.compilation_core).await
        }).await;

        if let Err(e) = client
            .communicator
            .send_to_conductor(message::i2c::Message::NotifyCompilationStatus(
                message::i2c::NotifyCompilationStatus {
                    submission_id,
                    result: compilation_result,
                },
            ))
            .await
        {
            println!("Failed to send to conductor: {:?}", e);
        }
    });

    Ok(())
}

async fn push_to_judgment_queue(
    message: message::c2i::PushToJudgementQueue,
    client: &Client,
) -> Result<(), errors::Error> {
    let submissions = client.submissions.read().await;

    let submission = submissions
        .get(&message.submission_id)
        .ok_or_else(|| {
            errors::ConductorFailure(format!(
                "Submission {} does not exist or has already been finalized",
                message.submission_id
            ))
        })?
        .clone();

    let communicator = client.communicator.clone();

    tokio::spawn(async move {
        let mut stream = submission.test_on_core(message.core, message.tests).await?;

        while let Some((test, judgement_result)) = stream.next().await {
            if let Err(e) = communicator
                .send_to_conductor(message::i2c::Message::NotifyTestStatus(
                    message::i2c::NotifyTestStatus {
                        submission_id: submission.id.clone(),
                        test,
                        judgement_result,
                    },
                ))
                .await
            {
                println!("Failed to send to conductor: {:?}", e);
            }
        }

        Ok(()) as Result<(), errors::Error>
    });

    Ok(())
}

async fn cancel_judgement_on_tests(
    message: message::c2i::CancelJudgementOnTests,
    client: Arc<Client>,
) -> Result<(), errors::Error> {
    let submissions = client.submissions.read().await;

    let submission = submissions.get(&message.submission_id).ok_or_else(|| {
        errors::ConductorFailure(format!(
            "Submission {} does not exist or has already been finalized",
            message.submission_id
        ))
    })?;

    submission.add_failed_tests(&message.failed_tests).await?;

    Ok(())
}

async fn finalize_submission(
    message: message::c2i::FinalizeSubmission,
    client: Arc<Client>,
) -> Result<(), errors::Error> {
    let mut submissions = client.submissions.write().await;

    let submission = submissions.remove(&message.submission_id).ok_or_else(|| {
        errors::ConductorFailure(format!(
            "Cannot finalize an unknown (or already finalized) submission {}",
            message.submission_id
        ))
    })?;

    submission.finalize().await?;

    Ok(())
}

async fn supply_file(
    message: message::c2i::SupplyFile,
    client: Arc<Client>,
) -> Result<(), errors::Error> {
    let tx = client
        .communicator
        .requests
        .lock()
        .await
        .remove(&message.request_id)
        .ok_or_else(|| {
            errors::ConductorFailure(format!(
                "Conductor sent reply to message #{}, which either does not exist or has been \
                 responded to already",
                message.request_id
            ))
        })?;

    tx.send(Ok(message.contents)).map_err(|_| {
        errors::InvokerFailure(format!(
            "Conductor sent reply to message #{}, but its handler is dead",
            message.request_id
        ))
    })
}

fn enter_sandbox() -> anyhow::Result<()> {
    // Various sanity checks
    let suid_dumpable = std::fs::read_to_string("/proc/sys/fs/suid_dumpable")?;
    if suid_dumpable == "2\n" {
        println!("suid_dumpable is set to 2 (suidsafe), which is potentially unsafe");
    } else if suid_dumpable != "0\n" {
        anyhow::bail!("suid_dumpable is not set to zero, unable to continue safely");
    }

    let fstype = nix::sys::statfs::statfs("/sys/fs/cgroup")
        .context("cgroups are not available at /sys/fs/cgroup")?
        .filesystem_type();
    match fstype.0 as i64 {
        libc::CGROUP2_SUPER_MAGIC => {}
        libc::TMPFS_MAGIC => {
            anyhow::bail!(
                "cgroups v1 seems to be mounted at /sys/fs/cgroup. sunwalker requires cgroups v2. \
                 Please configure your kernel and/or distribution to use cgroups v2"
            );
        }
        _ => {
            anyhow::bail!(
                "Unknown filesystem type at /sys/fs/cgroup. sunwalker requires cgroups v2. Please \
                 configure your kernel and/or distribution to use cgroups v2"
            );
        }
    }

    // Unshare namespaces
    if unsafe { libc::unshare(CLONE_NEWNS) } != 0 {
        anyhow::bail!("Initial unshare() failed, unable to continue safely");
    }

    // Do not propagate mounts
    system::change_propagation("/", system::MS_PRIVATE | system::MS_REC)
        .with_context(|| "Setting propagation of / to private recursively failed")?;

    // Mount tmpfs
    system::mount("none", "/tmp/sunwalker_invoker", "tmpfs", 0, None)
        .with_context(|| "Mounting tmpfs on /tmp/sunwalker_invoker failed")?;

    // Make various temporary directories
    std::fs::create_dir("/tmp/sunwalker_invoker/worker")
        .with_context(|| "Creating /tmp/sunwalker_invoker/worker failed")?;
    std::fs::create_dir("/tmp/sunwalker_invoker/submissions")
        .with_context(|| "Creating /tmp/sunwalker_invoker/submissions failed")?;
    std::fs::create_dir("/tmp/sunwalker_invoker/artifacts")
        .with_context(|| "Creating /tmp/sunwalker_invoker/artifacts failed")?;
    std::fs::create_dir("/tmp/sunwalker_invoker/emptydir")
        .with_context(|| "Creating /tmp/sunwalker_invoker/emptydir failed")?;

    // Prepare a copy of /dev
    std::fs::create_dir("/tmp/sunwalker_invoker/dev")
        .with_context(|| "Creating /tmp/sunwalker_invoker/dev failed")?;
    for name in [
        "null", "full", "zero", "urandom", "random", "stdin", "stdout", "stderr", "fd",
    ] {
        let source = format!("/dev/{name}");
        let target = format!("/tmp/sunwalker_invoker/dev/{name}");
        let metadata = std::fs::symlink_metadata(&source)
            .with_context(|| format!("{source} does not exist (or oculd not be accessed)"))?;
        if metadata.is_symlink() {
            let symlink_target = std::fs::read_link(&source)
                .with_context(|| format!("Cannot readlink {source:?}"))?;
            std::os::unix::fs::symlink(&symlink_target, &target)
                .with_context(|| format!("Cannot ln -s {symlink_target:?} {target:?}"))?;
            continue;
        } else if metadata.is_dir() {
            std::fs::create_dir(&target).with_context(|| format!("Cannot mkdir {target:?}"))?;
        } else {
            std::fs::File::create(&target).with_context(|| format!("Cannot touch {target:?}"))?;
        }
        system::bind_mount(&source, &target)
            .with_context(|| format!("Bind-mounting {source} to {target} failed"))?;
    }

    // These directories and files will be mounted onto later
    std::fs::create_dir("/tmp/sunwalker_invoker/dev/mqueue")
        .with_context(|| "Cannot mkdir /tmp/sunwalker_invoker/dev/mqueue")?;
    std::fs::create_dir("/tmp/sunwalker_invoker/dev/shm")
        .with_context(|| "Cannot mkdir /tmp/sunwalker_invoker/dev/shm")?;
    std::fs::create_dir("/tmp/sunwalker_invoker/dev/pts")
        .with_context(|| "Cannot mkdir /tmp/sunwalker_invoker/dev/pts")?;
    std::fs::write("/tmp/sunwalker_invoker/dev/ptmx", "")
        .with_context(|| "Cannot touch /tmp/sunwalker_invoker/dev/ptmx")?;

    Ok(())
}
