use anyhow::{anyhow, Context, Result};
use libc::{c_int, F_SETFD, O_CLOEXEC};
use serde::{
    de::DeserializeOwned, ser::SerializeStruct, Deserialize, Deserializer, Serialize, Serializer,
};
use std::marker::PhantomData;
use std::os::unix::io::{AsRawFd, FromRawFd};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub enum Direction {
    ParentToChild,
    ChildToParent,
}

pub struct Sender<T: Serialize> {
    fd: Option<File>,
    marker: PhantomData<T>,
}

pub struct Receiver<T: DeserializeOwned> {
    fd: File,
    marker: PhantomData<T>,
}

pub fn channel<T: Serialize + DeserializeOwned>(
    dir: Direction,
) -> Result<(Sender<T>, Receiver<T>)> {
    let mut fds: [c_int; 2] = [0; 2];
    let ret = unsafe { libc::pipe2(&mut fds as *mut c_int, O_CLOEXEC) };
    if ret == -1 {
        Err(std::io::Error::last_os_error())
            .with_context(|| "Failed to create an IPC pipe: pipe2 failed")?;
    }
    // Unset CLOEXEC on one of the endpoints. This seems safer than invoking pipe2(0) and then
    // setting CLOEXEC on the other one, because this avoids a (purely theoretical, at the moment)
    // race condition that may lead to a leak of fd.
    let ret = unsafe {
        libc::fcntl(
            fds[match dir {
                Direction::ParentToChild => 0,
                Direction::ChildToParent => 1,
            }],
            F_SETFD,
            0,
        )
    };
    if ret == -1 {
        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
        Err(std::io::Error::last_os_error())
            .with_context(|| "Failed to create an IPC pipe: fcntl failed")?;
    }
    // This is not expected to fail:
    let rx = unsafe { File::from_raw_fd(fds[0]) };
    let tx = unsafe { File::from_raw_fd(fds[1]) };
    Ok((
        Sender {
            fd: Some(tx),
            marker: PhantomData,
        },
        Receiver {
            fd: rx,
            marker: PhantomData,
        },
    ))
}

impl<T: Serialize> Sender<T> {
    // Consuming 'value' is a way of saying we transfer ownership
    pub async fn send(&mut self, value: T) -> Result<()> {
        let fd = self
            .fd
            .as_mut()
            .ok_or_else(|| anyhow!("Sender is closed"))?;
        let serialized = rmp_serde::to_vec(&value)?;
        fd.write_all(&serialized.len().to_ne_bytes()).await?;
        fd.write_all(&serialized).await?;
        Ok(())
    }
}

impl<T: DeserializeOwned> Receiver<T> {
    pub async fn recv(&mut self) -> Result<Option<T>> {
        let mut sz = [0u8; std::mem::size_of::<usize>()];
        if let Err(e) = self.fd.read_exact(&mut sz).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            Err(e)?;
        }

        let mut serialized = vec![0u8; usize::from_ne_bytes(sz)];
        self.fd.read_exact(&mut serialized[..]).await?;

        Ok(Some(rmp_serde::from_read_ref(&serialized)?))
    }
}

impl<T: Serialize> Serialize for Sender<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Sender", 1)?;
        state.serialize_field("fd", &self.fd.as_ref().map(|fd| fd.as_raw_fd()))?;
        state.end()
    }
}

impl<T: DeserializeOwned> Serialize for Receiver<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Receiver", 1)?;
        state.serialize_field("fd", &self.fd.as_raw_fd())?;
        state.end()
    }
}

impl<'de, T: Serialize> Deserialize<'de> for Sender<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Sender<T>, D::Error> {
        let fd = {
            #[derive(Deserialize)]
            struct Sender {
                fd: Option<c_int>,
            }
            <Sender as Deserialize<'de>>::deserialize(deserializer)?.fd
        };
        Ok(Sender {
            fd: fd.map(|fd| unsafe { File::from_raw_fd(fd) }),
            marker: PhantomData,
        })
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for Receiver<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Receiver<T>, D::Error> {
        let fd = {
            #[derive(Deserialize)]
            struct Receiver {
                fd: c_int,
            }
            <Receiver as Deserialize<'de>>::deserialize(deserializer)?.fd
        };
        Ok(Receiver {
            fd: unsafe { File::from_raw_fd(fd) },
            marker: PhantomData,
        })
    }
}
