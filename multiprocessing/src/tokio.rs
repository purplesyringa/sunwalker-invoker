use crate::{
    imp, ipc::MAX_PACKET_SIZE, Await, Deserialize, Deserializer, FnOnce, Object, Serialize,
    Serializer,
};
use std::io::{Error, ErrorKind, IoSlice, IoSliceMut, Result};
use std::marker::PhantomData;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use tokio_seqpacket::{
    ancillary::{AncillaryData, SocketAncillary},
    UnixSeqpacket,
};

#[derive(Object)]
pub struct Sender<T: Serialize> {
    fd: UnixSeqpacket,
    marker: PhantomData<fn(T) -> T>,
}

#[derive(Object)]
pub struct Receiver<T: Deserialize> {
    fd: UnixSeqpacket,
    marker: PhantomData<fn(T) -> T>,
}

#[derive(Object)]
pub struct Duplex<S: Serialize, R: Deserialize> {
    sender: Sender<S>,
    receiver: Receiver<R>,
}

pub fn channel<T: Serialize + Deserialize>() -> Result<(Sender<T>, Receiver<T>)> {
    let (tx, rx) = UnixSeqpacket::pair()?;
    Ok((
        Sender::from_unix_seqpacket(tx),
        Receiver::from_unix_seqpacket(rx),
    ))
}

pub fn duplex<A: Serialize + Deserialize, B: Serialize + Deserialize>(
) -> Result<(Duplex<A, B>, Duplex<B, A>)> {
    let (a_tx, a_rx) = channel::<A>()?;
    let (b_tx, b_rx) = channel::<B>()?;
    Ok((
        Duplex {
            sender: a_tx,
            receiver: b_rx,
        },
        Duplex {
            sender: b_tx,
            receiver: a_rx,
        },
    ))
}

impl<T: Serialize> Sender<T> {
    pub fn from_unix_seqpacket(fd: UnixSeqpacket) -> Self {
        Sender {
            fd,
            marker: PhantomData,
        }
    }

    pub async fn send(&mut self, value: &T) -> Result<()> {
        let mut s = Serializer::new();
        s.serialize(value);

        let fds = s.drain_fds();
        let serialized = s.into_vec();

        let mut ancillary_buffer = [0; 253];

        // Send the data and pass file descriptors
        let mut buffer_pos: usize = 0;
        let mut fds_pos: usize = 0;

        loop {
            let buffer_end = serialized.len().min(buffer_pos + MAX_PACKET_SIZE - 1);
            let fds_end = fds.len().min(fds_pos + 253);

            let is_last = buffer_end == serialized.len() && fds_end == fds.len();

            let mut ancillary = SocketAncillary::new(&mut ancillary_buffer);
            if !ancillary.add_fds(&fds[fds_pos..fds_end]) {
                return Err(Error::new(ErrorKind::Other, "Too many fds to pass"));
            }

            let n_written = self
                .fd
                .send_vectored_with_ancillary(
                    &[
                        IoSlice::new(&[is_last as u8]),
                        IoSlice::new(&serialized[buffer_pos..buffer_end]),
                    ],
                    &mut ancillary,
                )
                .await?;
            buffer_pos += n_written - 1;
            fds_pos = fds_end;

            if is_last {
                break;
            }
        }

        Ok(())
    }
}

impl<T: Serialize> AsRawFd for Sender<T> {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl<T: Serialize> FromRawFd for Sender<T> {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        imp::enable_nonblock(fd).expect("Failed to set O_NONBLOCK");
        Self::from_unix_seqpacket(UnixSeqpacket::from_raw_fd(fd).expect(
            "Failed to register fd in tokio in multiprocessing::tokio::Sender::from_raw_fd",
        ))
    }
}

impl<T: Deserialize> Receiver<T> {
    pub fn from_unix_seqpacket(fd: UnixSeqpacket) -> Self {
        Receiver {
            fd,
            marker: PhantomData,
        }
    }

    pub async fn recv(&mut self) -> Result<Option<T>> {
        // Read the data and the passed file descriptors
        let mut serialized: Vec<u8> = Vec::new();
        let mut buffer_pos: usize = 0;

        let mut ancillary_buffer = [0; 253];
        let mut received_fds: Vec<OwnedFd> = Vec::new();

        loop {
            serialized.resize(buffer_pos + MAX_PACKET_SIZE - 1, 0);

            let mut marker = [0];
            let mut ancillary = SocketAncillary::new(&mut ancillary_buffer[..]);

            let n_read = self
                .fd
                .recv_vectored_with_ancillary(
                    &mut [
                        IoSliceMut::new(&mut marker),
                        IoSliceMut::new(&mut serialized[buffer_pos..]),
                    ],
                    &mut ancillary,
                )
                .await?;

            for cmsg in ancillary.messages() {
                if let Ok(AncillaryData::ScmRights(rights)) = cmsg {
                    for fd in rights {
                        received_fds.push(unsafe { OwnedFd::from_raw_fd(fd) });
                    }
                } else {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("Unexpected kind of cmsg on stream"),
                    ));
                }
            }

            if ancillary.is_empty() && n_read == 0 {
                if buffer_pos == 0 && received_fds.is_empty() {
                    return Ok(None);
                } else {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("Unterminated data on stream"),
                    ));
                }
            }

            if n_read == 0 {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("Unexpected empty message on stream"),
                ));
            }

            buffer_pos += n_read - 1;
            if marker[0] == 1 {
                break;
            }
        }

        serialized.truncate(buffer_pos);

        let mut d = Deserializer::from(serialized, received_fds);
        Ok(Some(d.deserialize()))
    }
}

impl<T: Deserialize> AsRawFd for Receiver<T> {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl<T: Deserialize> FromRawFd for Receiver<T> {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        imp::enable_nonblock(fd).expect("Failed to set O_NONBLOCK");
        Self::from_unix_seqpacket(UnixSeqpacket::from_raw_fd(fd).expect(
            "Failed to register fd in tokio in multiprocessing::tokio::Receiver::from_raw_fd",
        ))
    }
}

impl<S: Serialize, R: Deserialize> Duplex<S, R> {
    pub async fn send(&mut self, value: &S) -> Result<()> {
        self.sender.send(value).await
    }
    pub async fn recv(&mut self) -> Result<Option<R>> {
        self.receiver.recv().await
    }
}

pub struct Child<T: Deserialize> {
    proc: tokio::process::Child,
    output_rx: Receiver<T>,
}

impl<T: Deserialize> Child<T> {
    pub fn new(proc: tokio::process::Child, output_rx: Receiver<T>) -> Child<T> {
        Child { proc, output_rx }
    }

    pub async fn kill(&mut self) -> Result<()> {
        self.proc.kill().await
    }

    pub fn id(&mut self) -> u32 {
        self.proc.id().expect(
            "multiprocessing::tokio::Child::id() cannot be called after the process is terminated",
        )
    }

    pub async fn join(&mut self) -> Result<T> {
        let value = self.output_rx.recv().await?;
        if self.proc.wait().await?.success() {
            value.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "The subprocess terminated without returning a value",
                )
            })
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "The subprocess did not terminate successfully",
            ))
        }
    }
}

pub async fn spawn<T: Object, U: Await<T>>(
    entry: Box<dyn FnOnce<(RawFd, RawFd), Output = i32>>,
    f: Box<dyn FnOnce<(), Output = U>>,
) -> Result<Child<T>> {
    let (mut entry_tx, entry_rx) = channel::<Box<dyn FnOnce<(RawFd, RawFd), Output = i32>>>()?;
    let entry_rx_fd = entry_rx.as_raw_fd();

    let (mut input_tx, input_rx) = channel::<Box<dyn FnOnce<(), Output = U>>>()?;
    let input_rx_fd = input_rx.as_raw_fd();

    let (output_tx, output_rx) = channel::<T>()?;
    let output_tx_fd = output_tx.as_raw_fd();

    let mut command = tokio::process::Command::new("/proc/self/exe");
    let child = unsafe {
        command
            .arg0("_multiprocessing_")
            .arg(entry_rx_fd.to_string())
            .arg(input_rx_fd.to_string())
            .arg(output_tx_fd.to_string())
            .pre_exec(move || {
                imp::disable_cloexec(entry_rx_fd)?;
                imp::disable_cloexec(input_rx_fd)?;
                imp::disable_cloexec(output_tx_fd)?;
                Ok(())
            })
            .spawn()?
    };

    entry_tx.send(&entry).await?;
    input_tx.send(&f).await?;

    Ok(Child::new(child, output_rx))
}
