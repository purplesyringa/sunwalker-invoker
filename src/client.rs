use crate::{cgroups, config, errors, image, init, message, problem, submission, system};
use anyhow::Context;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::SinkExt;
use futures_util::StreamExt;
use libc::CLONE_NEWNS;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

struct Client {
    config: config::Config,
    submissions: RwLock<HashMap<String, submission::Submission>>,
    problem_store: problem::store::ProblemStore,
    mounted_image: Arc<image::image::Image>,
    ephemeral_disk_space: u64,
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
}

impl Client {
    async fn send_to_conductor(&self, message: message::i2c::Message) -> Result<(), errors::Error> {
        self.conductor_write
            .lock()
            .await
            .send(tungstenite::Message::Binary(
                rmp_serde::to_vec(&message).map_err(|e| {
                    errors::CommunicationError(format!(
                        "Failed to serialize a message to conductor: {:?}",
                        e
                    ))
                })?,
            ))
            .await
            .map_err(|e| {
                errors::CommunicationError(format!(
                    "Failed to send a message to conductor via websocket: {:?}",
                    e
                ))
            })?;
        Ok(())
    }
}

#[tokio::main]
pub async fn client_main(cli_args: init::CLIArgs) -> anyhow::Result<()> {
    let config = std::fs::read_to_string(&cli_args.config)
        .with_context(|| format!("Failed to read config from {}", cli_args.config))?;
    let config: config::Config = toml::from_str(&config).with_context(|| "Config is invalid")?;

    enter_sandbox()?;

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

    let mut cpusets = Vec::new();
    for core in &config.environment.cpu_cores {
        cpusets.push(
            cgroups::AffineCPUSet::new(*core)
                .with_context(|| format!("Failed to create cpuset for core {}", core))?,
        );
    }

    // let worker_space = image::sandbox::enter_worker_space(image::sandbox::SandboxConfig {
    //     max_size_in_bytes: 8 * 1024 * 1024,
    //     max_inodes: 1024,
    //     core: 2,
    // })?;
    // std::fs::create_dir("/tmp/artifacts/test")?;
    // std::fs::copy(
    //     "/home/ivanq/Documents/sunwalker_invoker/hello",
    //     "/tmp/artifacts/test/hello",
    // )?;
    // let package = image::package::Package::new(mounted_image, "gcc".to_string())?;
    // let rootfs = worker_space
    //     .make_rootfs(
    //         &package,
    //         vec![(
    //             "/home/ivanq/Documents/sunwalker_invoker/hello"
    //                 .to_string()
    //                 .into(),
    //             "/space/hello".to_string(),
    //         )],
    //         "run".to_string(),
    //     )
    //     .map_err(|e| {
    //         errors::InvokerFailure(format!("Failed to make sandbox for running: {:?}", e))
    //     })?;

    // for _ in 0..1000 {
    //     use multiprocessing::Bind;

    //     let (mut upstream, downstream) =
    //         multiprocessing::tokio::duplex::<(), ()>().map_err(|e| {
    //             errors::InvokerFailure(format!(
    //                 "Failed to create duplex connection to an isolated subprocess: {:?}",
    //                 e
    //             ))
    //         })?;

    //     let mut child = isolated_entry
    //         .spawn_tokio(downstream, "run".to_string())
    //         .await
    //         .map_err(|e| {
    //             errors::InvokerFailure(format!("Failed to start an isolated subprocess: {:?}", e))
    //         })?;

    //     child.join().await.map_err(|e| {
    //         errors::InvokerFailure(format!(
    //             "Isolated process didn't terminate gracefully: {:?}",
    //             e
    //         ))
    //     })?;
    // }

    // return Ok(());

    let problem_store =
        problem::store::ProblemStore::new(std::path::PathBuf::from(&config.cache.problems))
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
        conductor_write: Mutex::new(conductor_write),
        conductor_read: Mutex::new(conductor_read),
    });

    // Handshake
    client
        .send_to_conductor(message::i2c::Message::Handshake(message::i2c::Handshake {
            invoker_name: client.config.invoker.name.clone(),
        }))
        .await?;

    // Initial mode
    client
        .send_to_conductor(message::i2c::Message::UpdateMode(
            message::i2c::UpdateMode {
                added_cores: client.config.environment.cpu_cores.clone(),
                removed_cores: Vec::new(),
                designated_ram: 0,
            },
        ))
        .await?;

    while let Some(message) = client.conductor_read.lock().await.next().await {
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
        Message::PushToJudgementQueue(message) => push_to_judgment_queue(message, client).await?,
        Message::CancelJudgementOnTests(message) => {
            cancel_judgement_on_tests(message, client).await?
        }
        Message::FinalizeSubmission(message) => finalize_submission(message, client).await?,
    }

    Ok(())
}

async fn add_submission(
    message: message::c2i::AddSubmission,
    client: Arc<Client>,
) -> Result<(), errors::Error> {
    if !client
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

    if !client.mounted_image.has_language(&message.language) {
        return Err(errors::ConductorFailure(format!(
            "Language {} is not available",
            message.language
        )));
    }

    // tokio::spawn(async move {
    let problem = client.problem_store.load(&message.problem_id).await?;

    let mut submission = submission::Submission::new(
        message.submission_id.clone(),
        problem.dependency_dag.clone(),
        image::image::Image::get_language(client.mounted_image.clone(), message.language.clone())?,
    )?;

    for (name, content) in message.files.into_iter() {
        submission.add_source_file(&name, &content)?;
    }

    let mut submissions = client.submissions.write().await;
    let submission = submissions
        .try_insert(message.submission_id.clone(), submission)
        .map_err(|_| {
            errors::ConductorFailure(format!(
                "A submission with ID {} cannot be added because it is already in the queue",
                message.submission_id
            ))
        })?;

    let compilation_result = submission.compile_on_core(message.compilation_core).await;

    match compilation_result {
        Ok(log) => {
            client
                .send_to_conductor(message::i2c::Message::NotifyCompilationStatus(
                    message::i2c::NotifyCompilationStatus {
                        submission_id: message.submission_id,
                        success: true,
                        log,
                    },
                ))
                .await
        }
        Err(errors::UserFailure(log)) => {
            client
                .send_to_conductor(message::i2c::Message::NotifyCompilationStatus(
                    message::i2c::NotifyCompilationStatus {
                        submission_id: message.submission_id,
                        success: false,
                        log,
                    },
                ))
                .await
        }
        Err(e) => Err(e),
    }
    // });

    // Ok(())
}

async fn push_to_judgment_queue(
    message: message::c2i::PushToJudgementQueue,
    client: Arc<Client>,
) -> Result<(), errors::Error> {
    let submissions = client.submissions.read().await;

    let submission = submissions.get(&message.submission_id).ok_or_else(|| {
        errors::ConductorFailure(format!(
            "Submission {} does not exist or has already been finalized",
            message.submission_id
        ))
    })?;

    for test in message.tests {
        submission
            .schedule_test_on_core(message.core, test)
            .await
            .map_err(|e| {
                errors::InvokerFailure(format!(
                    "Failed to schedule evaluation of test {} on core {}: {:?}",
                    test, message.core, e
                ))
            })?;
    }

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

    let mut submission = submissions.remove(&message.submission_id).ok_or_else(|| {
        errors::ConductorFailure(format!(
            "Cannot finalize an unknown (or already finalized) submission {}",
            message.submission_id
        ))
    })?;

    submission.finalize().await?;

    Ok(())
}

fn enter_sandbox() -> anyhow::Result<()> {
    // Unshare namespaces
    if unsafe { libc::unshare(CLONE_NEWNS) } != 0 {
        anyhow::bail!("Initial unshare() failed, unable to continue safely");
    }

    // Do not propagate mounts
    system::change_propagation("/", system::MS_PRIVATE | system::MS_REC)
        .with_context(|| "Setting propagation of / to private recursively failed")?;

    // Mount tmpfs
    system::mount("none", "/tmp", "tmpfs", 0, None)
        .with_context(|| "Mounting tmpfs on /tmp failed")?;

    // Make various temporary directories
    std::fs::create_dir("/tmp/worker").with_context(|| "Creating /tmp/worker failed")?;
    std::fs::create_dir("/tmp/submissions").with_context(|| "Creating /tmp/submissions failed")?;
    std::fs::create_dir("/tmp/artifacts").with_context(|| "Creating /tmp/artifacts failed")?;

    // Prepare a copy of /dev
    std::fs::create_dir("/tmp/dev").with_context(|| "Creating /tmp/dev failed")?;
    for name in [
        "null", "full", "zero", "urandom", "random", "stdin", "stdout", "stderr", "shm", "mqueue",
        "ptmx", "pts", "fd",
    ] {
        let source = format!("/dev/{}", name);
        let target = format!("/tmp/dev/{}", name);
        let metadata = std::fs::symlink_metadata(&source)
            .with_context(|| format!("{} does not exist (or oculd not be accessed)", source))?;
        if metadata.is_symlink() {
            let symlink_target = std::fs::read_link(&source)
                .with_context(|| format!("Cannot readlink {:?}", source))?;
            std::os::unix::fs::symlink(&symlink_target, &target)
                .with_context(|| format!("Cannot ln -s {:?} {:?}", symlink_target, target))?;
            continue;
        } else if metadata.is_dir() {
            std::fs::create_dir(&target).with_context(|| format!("Cannot mkdir {:?}", target))?;
        } else {
            std::fs::File::create(&target).with_context(|| format!("Cannot touch {:?}", target))?;
        }
        system::bind_mount(&source, &target)
            .with_context(|| format!("Bind-mounting {} -> {} failed", target, source))?;
    }

    Ok(())
}

// #[multiprocessing::entrypoint]
// #[tokio::main(flavor = "current_thread")] // unshare requires a single thread
// async fn isolated_entry(
//     mut duplex: multiprocessing::tokio::Duplex<(), ()>,
//     id: String,
// ) -> Result<(), errors::Error> {
//     // Unshare namespaces
//     if unsafe {
//         libc::unshare(
//             libc::CLONE_NEWNS
//                 | libc::CLONE_NEWNET
//                 | libc::CLONE_NEWUSER
//                 | libc::CLONE_NEWUTS
//                 | libc::CLONE_SYSVSEM
//                 | libc::CLONE_NEWPID,
//         )
//     } != 0
//     {
//         return Err(errors::InvokerFailure(format!(
//             "Failed to unshare mount namespace: {:?}",
//             std::io::Error::last_os_error()
//         )));
//     }

//     println!("duh");

//     Ok(())
// }
