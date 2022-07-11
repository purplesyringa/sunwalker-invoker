use crate::{errors, errors::ToResult, message};
use async_stream::try_stream;
use futures::stream::{SplitSink, SplitStream, Stream};
use futures_util::SinkExt;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic;
use tokio::sync::{oneshot, Mutex};
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

impl Communicator {
    pub async fn connect(conductor_address: &str) -> Result<Communicator, errors::Error> {
        let (conductor_ws, _) = tokio_tungstenite::connect_async(conductor_address)
            .await
            .context_invoker("Failed to connect to the conductor via a websocket")?;

        let (conductor_write, conductor_read) = conductor_ws.split();

        Ok(Communicator {
            conductor_read: Mutex::new(conductor_read),
            conductor_write: Mutex::new(conductor_write),
            next_request_id: atomic::AtomicU64::new(0),
            requests: Mutex::new(HashMap::new()),
        })
    }

    pub async fn send_to_conductor(
        &self,
        message: message::i2c::Message,
    ) -> Result<(), errors::Error> {
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
        if let Err(e) = std::fs::remove_dir_all(target_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(e).context_invoker("Failed to delete target directory");
            }
        }
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

    pub fn messages<'a>(
        &'a self,
    ) -> impl Stream<Item = Result<message::c2i::Message, errors::Error>> + 'a {
        try_stream! {
            let mut conductor_read = self.conductor_read.lock().await;

            while let Some(message) = conductor_read.next().await {
                let message = message.map_err(|e| {
                    errors::CommunicationError(format!(
                        "Failed to read message from the conductor: {e:?}"
                    ))
                })?;
                match message {
                    tungstenite::Message::Close(_) => break,
                    tungstenite::Message::Binary(buf) => {
                        yield rmp_serde::from_slice(&buf).map_err(|e| {
                            errors::CommunicationError(format!(
                                "Failed to parse buffer as msgpack format: {e:?}"
                            ))
                        })?;
                    }
                    tungstenite::Message::Ping(_) => (),
                    _ => {
                        println!("Message of unknown type received from the conductor: {message:?}")
                    }
                };
            }
        }
    }

    pub async fn supply_file(&self, message: message::c2i::SupplyFile) {
        match self.requests.lock().await.remove(&message.request_id) {
            Some(tx) => {
                if let Err(_) = tx.send(Ok(message.contents)) {
                    println!(
                        "Conductor sent reply to message #{} of kind RequestFile, but its handler is \
                         dead",
                        message.request_id
                    );
                }
            }
            None => {
                println!(
                    "Conductor sent reply to message #{} of kind RequestFile, which either does not \
                     exist or has been responded to already",
                    message.request_id
                );
            }
        }
    }
}
