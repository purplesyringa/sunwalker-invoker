use crate::{Deserializer, SerializeSafe, Serializer};
use std::io::{Error, ErrorKind, IoSlice, IoSliceMut, Result};
use std::marker::PhantomData;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use tokio_seqpacket::{
    ancillary::{AncillaryData, SocketAncillary},
    UnixSeqpacket,
};

#[derive(SerializeSafe)]
pub struct Sender<T: SerializeSafe> {
    fd: UnixSeqpacket,
    marker: PhantomData<T>,
}

#[derive(SerializeSafe)]
pub struct Receiver<T: SerializeSafe> {
    fd: UnixSeqpacket,
    marker: PhantomData<T>,
}

#[derive(SerializeSafe)]
pub struct Duplex<S: SerializeSafe, R: SerializeSafe> {
    sender: Sender<S>,
    receiver: Receiver<R>,
}

pub fn channel<T: SerializeSafe>() -> Result<(Sender<T>, Receiver<T>)> {
    let (tx, rx) = UnixSeqpacket::pair()?;
    Ok((
        Sender::from_unix_seqpacket(tx),
        Receiver::from_unix_seqpacket(rx),
    ))
}

pub fn duplex<A: SerializeSafe, B: SerializeSafe>() -> Result<(Duplex<A, B>, Duplex<B, A>)> {
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

impl<T: SerializeSafe> Sender<T> {
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

impl<T: SerializeSafe> AsRawFd for Sender<T> {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl<T: SerializeSafe> FromRawFd for Sender<T> {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self::from_unix_seqpacket(UnixSeqpacket::from_raw_fd(fd).unwrap())
    }
}

impl<T: SerializeSafe> Receiver<T> {
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

impl<T: SerializeSafe> AsRawFd for Receiver<T> {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl<T: SerializeSafe> FromRawFd for Receiver<T> {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self::from_unix_seqpacket(UnixSeqpacket::from_raw_fd(fd).unwrap())
    }
}

impl<S: SerializeSafe, R: SerializeSafe> Duplex<S, R> {
    pub async fn send(&mut self, value: &S) -> Result<()> {
        self.sender.send(value).await
    }
    pub async fn recv(&mut self) -> Result<Option<R>> {
        self.receiver.recv().await
    }
}
