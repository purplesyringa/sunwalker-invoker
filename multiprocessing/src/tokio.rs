use crate::{Deserialize, Deserializer, Object, Serialize, Serializer};
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
    marker: PhantomData<T>,
}

#[derive(Object)]
pub struct Receiver<T: Deserialize> {
    fd: UnixSeqpacket,
    marker: PhantomData<T>,
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

        // Send the size of data and pass file descriptors
        let fds = s.drain_fds();

        let serialized = s.into_vec();
        let sz = serialized.len().to_ne_bytes();

        let mut ancillary_buffer = [0; 253];
        let mut ancillary = SocketAncillary::new(&mut ancillary_buffer);
        if !ancillary.add_fds(&fds) {
            return Err(Error::new(ErrorKind::Other, "Too many fds to pass"));
        }

        let size = self
            .fd
            .send_vectored_with_ancillary(&[IoSlice::new(&sz)], &mut ancillary)
            .await?;
        if size != sz.len() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Expected to send {} bytes, sent {} bytes", sz.len(), size),
            ));
        }

        // Send data itself
        let mut serialized = &serialized[..];
        while !serialized.is_empty() {
            let n = self.fd.send(serialized).await?;
            if n == 0 {
                break;
            }
            serialized = &serialized[n..];
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
        // Read size of data and the passed file descriptors
        let mut sz = [0u8; std::mem::size_of::<usize>()];

        let mut ancillary_buffer = [0; 253];
        let mut ancillary = SocketAncillary::new(&mut ancillary_buffer[..]);

        let size = self
            .fd
            .recv_vectored_with_ancillary(&mut [IoSliceMut::new(&mut sz)], &mut ancillary)
            .await?;
        if size == 0 {
            return Ok(None);
        }

        if size != sz.len() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Expected to receive {} bytes, got {} bytes", sz.len(), size),
            ));
        }

        let mut received_fds: Vec<OwnedFd> = Vec::new();
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

        let mut serialized = vec![0u8; usize::from_ne_bytes(sz)];

        let mut buf = &mut serialized[..];
        while !buf.is_empty() {
            let n = self.fd.recv(buf).await?;
            if n == 0 {
                return Err(Error::new(ErrorKind::Other, format!("Unexpected EOF")));
            }
            buf = &mut buf[n..];
        }

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
