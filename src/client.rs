use crate::{
    cgroups, communicator, config, errors, image, init, message, problem, submission, system,
};
use anyhow::Context;
use futures_util::StreamExt;
use libc::CLONE_NEWNS;
use ouroboros::self_referencing;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard, RwLock};

pub struct Client {
    config: config::Config,
    submissions: RwLock<HashMap<String, Arc<submission::Submission>>>,
    problem_store: problem::store::ProblemStore,
    mounted_image: Arc<image::image::Image>,
    ephemeral_disk_space: u64,
    communicator: Arc<communicator::Communicator>,
    core_locks: HashMap<u64, Mutex<()>>,
}

#[self_referencing]
pub struct CoreHandle {
    core: u64,
    client: Arc<Client>,
    #[borrows(client)]
    #[covariant]
    guard: MutexGuard<'this, ()>,
}

impl CoreHandle {
    pub fn get_core(&self) -> u64 {
        // Shut rustc up about unused field. Naming it _guard just brings more warnings because of
        // non-snake-case function names that ouroboros creates.
        self.borrow_guard();

        *self.borrow_core()
    }
}

impl Client {
    fn try_lock_core(self: &Arc<Self>, core: u64) -> Result<CoreHandle, errors::Error> {
        CoreHandle::try_new(core, self.clone(), |client| {
            client
                .core_locks
                .get(&core)
                .ok_or_else(|| {
                    errors::ConductorFailure(format!(
                        "Core {core} is not dedicated to the invoker and cannot be used for a task"
                    ))
                })?
                .try_lock()
                .map_err(|_| {
                    errors::ConductorFailure(format!(
                        "Core {core} is already in use and can only be freed after submission \
                         finalization"
                    ))
                })
        })
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

    let communicator = Arc::new(
        communicator::Communicator::connect(&config.conductor.address)
            .await
            .with_context(|| {
                format!(
                    "Failed to connect to the conductor via a websocket at {:?} (this address is \
                     from field conductor.address of the configuration file)",
                    config.conductor.address
                )
            })?,
    );

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

    let core_locks = config
        .environment
        .cpu_cores
        .iter()
        .map(|core| (*core, Mutex::new(())))
        .collect();

    let client = Arc::new(Client {
        config,
        submissions: RwLock::new(HashMap::new()),
        problem_store,
        mounted_image,
        ephemeral_disk_space,
        communicator,
        core_locks,
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

    let messages = client.communicator.messages();
    futures::pin_mut!(messages);

    while let Some(message) = messages.next().await {
        handle_message(message?, &client).await;
    }

    Ok(())
}

async fn handle_message(message: message::c2i::Message, client: &Arc<Client>) {
    use message::c2i::*;

    println!("{:?}", message);

    match message {
        Message::AddSubmission(message) => add_submission(message, client).await,
        Message::PushToJudgementQueue(message) => push_to_judgment_queue(message, client).await,
        Message::CancelJudgementOnTests(message) => {
            cancel_judgement_on_tests(message, &client).await
        }
        Message::FinalizeSubmission(message) => finalize_submission(message, &client).await,
        Message::SupplyFile(message) => supply_file(message, &client).await,
    }
}

async fn add_submission(message: message::c2i::AddSubmission, client: &Arc<Client>) {
    match async {
        let core = client.try_lock_core(message.compilation_core)?;

        if !client.mounted_image.has_language(&message.language) {
            return Err(errors::ConductorFailure(format!(
                "Language {} is not available",
                message.language
            )));
        }

        let problem = client
            .problem_store
            .load_revision(message.problem_id, message.revision_id)
            .await?;

        let mut submission = submission::Submission::new(
            message.submission_id.clone(),
            problem,
            image::image::Image::get_language(
                client.mounted_image.clone(),
                message.language.clone(),
            )?,
            message.invocation_limits,
        )?;
        for (name, content) in message.files.into_iter() {
            submission.add_source_file(&name, &content)?;
        }

        let submission = Arc::new(submission);

        let mut submissions = client.submissions.write().await;
        submissions
            .try_insert(message.submission_id.clone(), submission.clone())
            .map_err(|_| {
                errors::ConductorFailure(format!(
                    "A submission with ID {} cannot be added because it is already in the queue",
                    message.submission_id
                ))
            })?;

        Ok((core, submission))
    }
    .await
    {
        Ok((core, submission)) => {
            let communicator = client.communicator.clone();
            tokio::spawn(async move {
                if let Err(e) = communicator
                    .send_to_conductor(message::i2c::Message::NotifyCompilationStatus(
                        message::i2c::NotifyCompilationStatus {
                            submission_id: message.submission_id,
                            result: submission.compile_on_core(core).await,
                        },
                    ))
                    .await
                {
                    println!("Failed to send to conductor: {:?}", e);
                }
            });
        }
        Err(e) => {
            if let Err(e) = client
                .communicator
                .send_to_conductor(message::i2c::Message::NotifyCompilationStatus(
                    message::i2c::NotifyCompilationStatus {
                        submission_id: message.submission_id,
                        result: Err(e),
                    },
                ))
                .await
            {
                println!("Failed to send to conductor: {:?}", e);
            }
        }
    }
}

async fn push_to_judgment_queue(message: message::c2i::PushToJudgementQueue, client: &Arc<Client>) {
    if let Err(e) = try {
        let submissions = client.submissions.read().await;

        let submission = submissions.get(&message.submission_id).ok_or_else(|| {
            errors::ConductorFailure(format!(
                "Submission {} does not exist or has already been finalized",
                message.submission_id
            ))
        })?;

        let core = client.try_lock_core(message.core)?;
        let submission_id = submission.id.clone();
        let communicator = client.communicator.clone();

        let mut stream = submission.test_on_core(core, message.tests).await?;

        tokio::spawn(async move {
            while let Some((test, judgement_result)) = stream.next().await {
                if let Err(e) = communicator
                    .send_to_conductor(message::i2c::Message::NotifyTestStatus(
                        message::i2c::NotifyTestStatus {
                            submission_id: submission_id.clone(),
                            test,
                            judgement_result,
                        },
                    ))
                    .await
                {
                    println!("Failed to send to conductor: {:?}", e);
                }
            }
        });
    } {
        if let Err(e) = client
            .communicator
            .send_to_conductor(message::i2c::Message::NotifySubmissionError(
                message::i2c::NotifySubmissionError {
                    submission_id: message.submission_id.clone(),
                    error: e,
                },
            ))
            .await
        {
            println!("Failed to send to conductor: {:?}", e);
        }
    }
}

async fn cancel_judgement_on_tests(message: message::c2i::CancelJudgementOnTests, client: &Client) {
    if let Err(e) = try {
        let submissions = client.submissions.read().await;

        let submission = submissions.get(&message.submission_id).ok_or_else(|| {
            errors::ConductorFailure(format!(
                "Submission {} does not exist or has already been finalized",
                message.submission_id
            ))
        })?;

        submission.add_failed_tests(&message.failed_tests).await?
    } {
        if let Err(e) = client
            .communicator
            .send_to_conductor(message::i2c::Message::NotifySubmissionError(
                message::i2c::NotifySubmissionError {
                    submission_id: message.submission_id.clone(),
                    error: e,
                },
            ))
            .await
        {
            println!("Failed to send to conductor: {:?}", e);
        }
    }
}

async fn finalize_submission(message: message::c2i::FinalizeSubmission, client: &Client) {
    if let Err(e) = try {
        let mut submissions = client.submissions.write().await;

        let submission = submissions.remove(&message.submission_id).ok_or_else(|| {
            errors::ConductorFailure(format!(
                "Cannot finalize an unknown (or already finalized) submission {}",
                message.submission_id
            ))
        })?;

        submission.finalize().await?
    } {
        if let Err(e) = client
            .communicator
            .send_to_conductor(message::i2c::Message::NotifySubmissionError(
                message::i2c::NotifySubmissionError {
                    submission_id: message.submission_id.clone(),
                    error: e,
                },
            ))
            .await
        {
            println!("Failed to send to conductor: {:?}", e);
        }
    }
}

async fn supply_file(message: message::c2i::SupplyFile, client: &Client) {
    client.communicator.supply_file(message).await;
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
