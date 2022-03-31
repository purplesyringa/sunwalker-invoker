use crate::{cgroups, config, image, init, message, problem, system, worker};
use anyhow::{anyhow, bail, Context, Result};
use futures_util::SinkExt;
use futures_util::StreamExt;
use libc::CLONE_NEWNS;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub struct Client {
    config: config::Config,
    core_workers: RwLock<HashMap<u64, Arc<worker::CoreWorker>>>,
    submissions: RwLock<HashMap<String, Arc<worker::SubmissionInfo>>>,
    problem_store: problem::store::ProblemStore,
    mounted_image: Arc<image::mount::MountedImage>,
    ephemeral_disk_space: u64,
}

pub fn client_main(cli_args: init::CLIArgs) -> Result<()> {
    let config = std::fs::read_to_string(&cli_args.config)
        .with_context(|| format!("Failed to read config from {}", cli_args.config))?;
    let config: config::Config = toml::from_str(&config).with_context(|| "Config is invalid")?;

    enter_sandbox()?;

    let image_cfg = std::fs::read_to_string(&config.image.config)
        .with_context(|| {
            format!("Could not read file image.cfg from path {} (this path is from field image.config of the configuration file)", config.image.config)
        })?;
    let image_cfg = image::config::Config::load(&image_cfg)
        .with_context(|| {
            format!("Could not load image.cfg as a script from path {} (this path is from field image.config of the configuration file)", config.image.config)
        })?;

    let mut mnt = image::mount::ImageMounter::new();
    let mounted_image = Arc::new(mnt
        .mount(&config.image.path, image_cfg)
        .with_context(|| {
            format!("Could not mount image.sfs from path {} (this path is from field image.path of the configuration file)", config.image.path)
        })?);

    cgroups::isolate_cores(&config.environment.cpu_cores)
        .with_context(|| {
            format!("Failed to isolate CPU cores {:?} (this list is from field environment.cpu_cores of the configuration file)", config.environment.cpu_cores)
        })?;

    let mut cpusets = Vec::new();
    for core in &config.environment.cpu_cores {
        cpusets.push(
            cgroups::AffineCPUSet::new(*core)
                .with_context(|| format!("Failed to create cpuset for core {}", core))?,
        );
    }

    let problem_store = problem::store::ProblemStore::new(std::path::PathBuf::from(&config.cache.problems))
            .with_context(|| {
                format!("Failed to create problem store with cache at {} (this path if from field cache.problems of the configuration file)", config.cache.problems)
            })?;

    let ephemeral_disk_space: u64 = config
        .environment
        .ephemeral_disk_space
        .clone()
        .try_into()
        .with_context(|| "Failed to parse environment.ephemeral_disk_space as size")?;

    let client = Arc::new(Client {
        config,
        core_workers: RwLock::new(HashMap::new()),
        submissions: RwLock::new(HashMap::new()),
        problem_store,
        mounted_image,
        ephemeral_disk_space,
    });
    main_loop(client)
}

#[tokio::main]
async fn main_loop(client: Arc<Client>) -> Result<()> {
    let (mut conductor_ws, _) = tokio_tungstenite::connect_async(&client.config.conductor.address).await
        .with_context(|| {
            format!("Failed to connect to the conductor via a websocket at {:?} (this address if from field conductor.address of the configuration file)", client.config.conductor.address)
        })?;

    println!(
        "Connected to the conductor at address {:?}",
        client.config.conductor.address
    );

    let handshake: message::i2c::Message =
        message::i2c::Message::Handshake(message::i2c::Handshake {
            invoker_name: client.config.invoker.name.clone(),
        });
    conductor_ws
        .send(tungstenite::Message::Binary(rmp_serde::to_vec(&handshake)?))
        .await?;

    while let Some(message) = conductor_ws.next().await {
        let message = message.with_context(|| "Failed to read message from the conductor")?;
        match message {
            tungstenite::Message::Close(_) => break,
            tungstenite::Message::Binary(buf) => {
                let message: message::c2i::Message = rmp_serde::from_slice(&buf)
                    .with_context(|| "Failed to parse buffer as msgpack format")?;
                handle_message(message, client.clone()).await?;
            }
            _ => println!(
                "Message of unknown type received from the conductor: {:?}",
                message
            ),
        };
    }

    Ok(())

    // let mut pool = corepool::CorePool::new(config.environment.cpu_cores)
    //     .with_context(|| "Could not create core pool")?;

    // std::fs::write(
    //     "/tmp/hello-world.cpp",
    //     "#include <bits/stdc++.h>\nint main() {\n\tstd::cout << \"Hello, world!\" << std::endl;\n}\n",
    // )?;

    // for i in 0..50 {
    //     let mounted_image = &mounted_image;
    //     pool.spawn_dedicated(corepool::Task {
    //         callback: Box::new(move || {
    //             let sandbox_config = image::sandbox::SandboxConfig {
    //                 max_size_in_bytes: ephemeral_disk_space,
    //                 max_inodes: config.environment.ephemeral_inodes,
    //                 bound_files: Vec::new(),
    //             };

    //             let lang = mounted_image
    //                 .get_language("c++.20.gcc")
    //                 .expect("Failed to get language c++.20.gcc");

    //             // println!(
    //             //     "{}",
    //             //     lang.identify(&sandbox_config)
    //             //         .expect("Failed to identify lang")
    //             // );

    //             let program = lang
    //                 .build(vec!["/tmp/hello-world.cpp"], &sandbox_config)
    //                 .expect("Failed to build /tmp/hello-world.cpp as c++.20.gcc");

    //             lang.get_ready_to_run(&sandbox_config)
    //                 .expect("Failed to get ready for running /tmp/hello-world.cpp as c++.20.gcc");

    //             lang.run(&sandbox_config, &program)
    //                 .expect("Failed to run /tmp/hello-world.cpp as c++.20.gcc");

    //             lang.run(&sandbox_config, &program)
    //                 .expect("Failed to run /tmp/hello-world.cpp as c++.20.gcc");

    //             lang.run(&sandbox_config, &program)
    //                 .expect("Failed to run /tmp/hello-world.cpp as c++.20.gcc");
    //         }),
    //         group: String::from("default"),
    //     })
    //     .unwrap();
    // }

    // pool.join();

    // println!("Spawned all");

    // // std::thread::sleep_ms(100000);
    // // start_worker();

    // Ok(())
}

async fn handle_message(message: message::c2i::Message, client: Arc<Client>) -> Result<()> {
    use message::c2i::*;

    println!("{:?}", message);

    match message {
        Message::AddSubmission(message) => add_submission(message, client).await?,
        Message::PushToJudgementQueue(message) => push_to_judgment_queue(message, client).await?,
        Message::CancelJudgementOnTests(message) => {
            cancel_judgement_on_tests(message, client).await?
        }
        Message::StopCores(message) => stop_cores(message, client).await?,
        Message::FinalizeSubmission(message) => finalize_submission(message, client).await?,
    }

    Ok(())
}

async fn add_submission(message: message::c2i::AddSubmission, client: Arc<Client>) -> Result<()> {
    if !client
        .config
        .environment
        .cpu_cores
        .contains(&message.compilation_core)
    {
        bail!(
            "Core {} is not dedicated to the invoker and cannot be scheduled for compilation of a new submission",
            message.compilation_core
        );
    }

    let root = format!("/tmp/submissions/{}", message.submission_id);
    std::fs::create_dir(&root).with_context(|| {
        format!(
            "Failed to create a directory for submission {} at {}",
            message.submission_id, root
        )
    })?;

    let mut source_files = Vec::new();
    for (name, content) in message.files.into_iter() {
        let path = root.clone() + "/" + &name;
        source_files.push(path.clone());
        std::fs::write(&path, content).with_context(|| {
            format!(
                "Failed to write a source code file for submission {} at {}",
                message.submission_id, path
            )
        })?;
    }

    tokio::spawn(async move {
        let problem = client.problem_store.load(&message.problem_id).await?;

        let submission_info = Arc::new(worker::SubmissionInfo {
            id: message.submission_id,
            dependency_dag: RwLock::new(problem.dependency_dag.clone()),
            language: image::mount::MountedImage::get_language(
                client.mounted_image.clone(),
                message.language.clone(),
            )
            .with_context(|| format!("Failed to get language {}", message.language))?,
            source_files,
            built_program: RwLock::new(None),
            workers: Mutex::new(HashMap::new()),
        });

        if let Some(_) = client
            .submissions
            .write()
            .await
            .insert(submission_info.id.clone(), submission_info.clone())
        {
            bail!(
                "A submission with ID {} cannot be added because it is already in the queue",
                submission_info.id
            );
        }

        let core_worker = Arc::new(worker::CoreWorker::new(
            message.compilation_core,
            submission_info.clone(),
            image::sandbox::SandboxConfig {
                max_size_in_bytes: client.ephemeral_disk_space,
                max_inodes: client.config.environment.ephemeral_inodes,
                core: message.compilation_core,
            },
        )?);

        {
            let mut workers = submission_info.workers.lock().await;
            workers.insert(message.compilation_core, Arc::downgrade(&core_worker));
        }

        {
            let mut lock = client.core_workers.write().await;
            if let Some(_) = lock.insert(message.compilation_core, core_worker.clone()) {
                bail!(
                    "Core {} is already in use and cannot be scheduled for compilation of a new submission",
                    message.compilation_core
                );
            }
        }

        core_worker.push(worker::CoreCommand::Compile)?;

        core_worker.work().await
    });

    Ok(())
}

async fn push_to_judgment_queue(
    message: message::c2i::PushToJudgementQueue,
    client: Arc<Client>,
) -> Result<()> {
    let mut core_workers = client.core_workers.write().await;

    let core_worker = match core_workers.get(&message.core) {
        Some(core_worker) => {
            if core_worker.submission_info.id != message.submission_id {
                bail!(
                    "Core {} is working on submission {} at the moment, cannot schedule judgement of the submission {} on the same core",
                    message.core, core_worker.submission_info.id, message.submission_id
                );
            }

            core_worker.clone()
        }
        None => {
            let submission_info = client
                .submissions
                .read()
                .await
                .get(&message.submission_id)
                .ok_or_else(|| {
                    anyhow!("PushToJudgementQueue called for an unknown (or already finalized) submission {}", message.submission_id)
                })?
                .clone();

            let core_worker = Arc::new(worker::CoreWorker::new(
                message.core,
                submission_info.clone(),
                image::sandbox::SandboxConfig {
                    core: message.core,
                    max_size_in_bytes: client.ephemeral_disk_space,
                    max_inodes: client.config.environment.ephemeral_inodes,
                },
            )?);
            core_workers.insert(message.core, core_worker.clone());

            submission_info
                .workers
                .lock()
                .await
                .insert(message.core, Arc::downgrade(&core_worker));

            core_worker
        }
    };

    for test in message.tests {
        core_worker.push(worker::CoreCommand::Test(test))?;
    }

    Ok(())
}

async fn cancel_judgement_on_tests(
    message: message::c2i::CancelJudgementOnTests,
    client: Arc<Client>,
) -> Result<()> {
    let lock = client.core_workers.read().await;
    let core_worker = lock
        .get(&message.core)
        .ok_or_else(|| {
            anyhow!(
                "Core {} was not initialized by the conductor beforehand (or has already been deinitialized)",
                message.core
            )
        })?;

    if core_worker.submission_info.id != message.submission_id {
        bail!(
            "Core {} is working on submission {} at the moment, cannot schedule judgement of the submission {} on the same core",
            message.core, core_worker.submission_info.id, message.submission_id
        );
    }

    {
        let mut dependency_dag = core_worker.submission_info.dependency_dag.write().await;
        for test in message.failed_tests {
            dependency_dag.fail_test(test);
        }
    }

    core_worker.abort_current_command_if_necessary().await?;

    Ok(())
}

async fn stop_cores(message: message::c2i::StopCores, client: Arc<Client>) -> Result<()> {
    let submissions = client.submissions.read().await;

    let submission_info = submissions.get(&message.submission_id).ok_or_else(|| {
        anyhow!(
            "Cannot stop cores for an unknown (or already finalized) submission {}",
            message.submission_id
        )
    })?;

    let mut workers = submission_info.workers.lock().await;
    for core in message.cores {
        let worker = workers.get(&core);
        match worker {
            Some(worker) => {
                if let Some(worker) = worker.upgrade() {
                    worker.abort_all()?;
                }
                workers.remove(&core);
            }
            None => bail!(
                "Core {} is not judging submission {} at the moment",
                core,
                message.submission_id
            ),
        }
    }

    Ok(())
}

async fn finalize_submission(
    message: message::c2i::FinalizeSubmission,
    client: Arc<Client>,
) -> Result<()> {
    let mut submissions = client.submissions.write().await;

    let submission_info = submissions.get(&message.submission_id).ok_or_else(|| {
        anyhow!(
            "Cannot finalize an unknown (or already finalized) submission {}",
            message.submission_id
        )
    })?;

    {
        let workers = submission_info.workers.lock().await;
        if !workers.is_empty() {
            bail!(
                "Cannot finalize submission {}, which is executed on cores {:?} at the moment",
                message.submission_id,
                workers.keys()
            );
        }
    }

    submissions.remove(&message.submission_id);

    Ok(())
}

fn enter_sandbox() -> Result<()> {
    // Unshare namespaces
    if unsafe { libc::unshare(CLONE_NEWNS) } != 0 {
        bail!("Initial unshare() failed, unable to continue safely");
    }

    // Do not propagate mounts
    system::change_propagation("/", system::MS_PRIVATE | system::MS_REC)
        .with_context(|| "Setting propagation of / to private recursively failed")?;

    // Mount tmpfs
    system::mount("none", "/tmp", "tmpfs", 0, None)
        .with_context(|| "Mounting tmpfs on /tmp failed")?;

    // Make various temporary directories
    std::fs::create_dir("/tmp/worker").with_context(|| "Creating /tmp/worker failed")?;

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
