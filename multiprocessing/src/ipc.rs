use crate::{Deserializer, SerializeSafe, Serializer};
use nix::cmsg_space;
use nix::sys::{socket, uio::IoVec}; //::{c_int, AF_UNIX, SOCK_CLOEXEC, SOCK_STREAM};
use std::fs::File;
use std::io::{Read, Result, Write};
use std::marker::PhantomData;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};
// use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(SerializeSafe)]
pub struct Sender<T: SerializeSafe> {
    fd: File,
    marker: PhantomData<T>,
}

#[derive(SerializeSafe)]
pub struct Receiver<T: SerializeSafe> {
    fd: File,
    marker: PhantomData<T>,
}

#[derive(SerializeSafe)]
pub struct Duplex<S: SerializeSafe, R: SerializeSafe> {
    sender: Sender<S>,
    receiver: Receiver<R>,
}

pub fn channel<T: SerializeSafe>() -> Result<(Sender<T>, Receiver<T>)> {
    // The two fds are indistinguishable, but the sender-receiver model is the classical one
    let (tx, rx) = socket::socketpair(
        socket::AddressFamily::Unix,
        socket::SockType::SeqPacket,
        None,
        socket::SockFlag::SOCK_CLOEXEC,
    )?;
    // Safety: this should not fail after successful socketpair
    unsafe { Ok((Sender::from_raw_fd(tx), Receiver::from_raw_fd(rx))) }
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
    pub unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Sender {
            fd: File::from_raw_fd(fd),
            marker: PhantomData,
        }
    }

    // Consuming 'value' is a way of saying we transfer ownership
    pub fn send(&mut self, value: T) -> Result<()> {
        let mut s = Serializer::new();
        s.serialize(&value);

        // Send the size of data and pass file descriptors
        let fds = s.drain_fds();

        let serialized = s.into_vec();
        let sz = serialized.len().to_ne_bytes();

        let cmsg = socket::ControlMessage::ScmRights(&fds);
        socket::sendmsg(
            self.fd.as_raw_fd(),
            &[IoVec::from_slice(&sz)],
            &[cmsg],
            socket::MsgFlags::empty(),
            None,
        )?;

        // Send data itself
        self.fd.write_all(&serialized)?;
        Ok(())
    }
}

impl<T: SerializeSafe> AsRawFd for Sender<T> {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl<T: SerializeSafe> Receiver<T> {
    pub unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Receiver {
            fd: File::from_raw_fd(fd),
            marker: PhantomData,
        }
    }

    pub fn recv(&mut self) -> Result<Option<T>> {
        // Read size of data and the passed file descriptors
        let mut sz = [0u8; std::mem::size_of::<usize>()];
        let mut cmsg = cmsg_space!([RawFd; 253]); // SCM_MAX_FD
        let recvmsg = socket::recvmsg(
            self.fd.as_raw_fd(),
            &[IoVec::from_mut_slice(&mut sz)],
            Some(&mut cmsg),
            socket::MsgFlags::MSG_WAITALL | socket::MsgFlags::MSG_CMSG_CLOEXEC,
        )?;

        if recvmsg.bytes == 0 {
            return Ok(None);
        }

        if recvmsg.bytes != sz.len() {
            panic!(
                "Expected to receive {} bytes, got {} bytes",
                sz.len(),
                recvmsg.bytes
            );
        }

        let mut received_fds: Vec<OwnedFd> = Vec::new();
        for cmsg in recvmsg.cmsgs() {
            match cmsg {
                socket::ControlMessageOwned::ScmRights(rights) => {
                    for fd in rights {
                        received_fds.push(unsafe { OwnedFd::from_raw_fd(fd) });
                    }
                }
                _ => panic!("Unexpected kind of cmsg on stream: {:?}", cmsg),
            }
        }

        let mut serialized = vec![0u8; usize::from_ne_bytes(sz)];
        self.fd.read_exact(&mut serialized[..])?;

        let mut d = Deserializer::from(serialized, received_fds);
        Ok(Some(d.deserialize()))
    }
}

impl<T: SerializeSafe> AsRawFd for Receiver<T> {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl<S: SerializeSafe, R: SerializeSafe> Duplex<S, R> {
    pub fn send(&mut self, value: S) -> Result<()> {
        self.sender.send(value)
    }
    pub fn recv(&mut self) -> Result<Option<R>> {
        self.receiver.recv()
    }
}
