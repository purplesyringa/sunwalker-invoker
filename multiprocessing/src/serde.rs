use std::os::unix::io::{OwnedFd, RawFd};

pub struct Serializer {
    data: Vec<u8>,
    fds: Option<Vec<RawFd>>,
}

impl Serializer {
    pub fn new() -> Self {
        Serializer {
            data: Vec::new(),
            fds: Option::from(Vec::new()),
        }
    }

    pub fn write(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    pub fn serialize<T: SerializeSafe>(&mut self, data: &T) {
        data.serialize_self(self)
    }

    pub fn add_fd(&mut self, fd: RawFd) -> usize {
        let fds = self.fds.as_mut().unwrap();
        fds.push(fd);
        fds.len() - 1
    }

    pub fn drain_fds(&mut self) -> Vec<RawFd> {
        self.fds.take().unwrap()
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }
}

impl IntoIterator for Serializer {
    type Item = u8;
    type IntoIter = <Vec<u8> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

pub struct Deserializer {
    data: Vec<u8>,
    fds: Vec<Option<OwnedFd>>,
    pos: usize,
}

impl Deserializer {
    pub fn from(data: Vec<u8>, fds: Vec<OwnedFd>) -> Self {
        Deserializer {
            data,
            fds: fds.into_iter().map(|fd| Some(fd)).collect(),
            pos: 0,
        }
    }

    pub fn read(&mut self, data: &mut [u8]) {
        data.clone_from_slice(&self.data[self.pos..self.pos + data.len()]);
        self.pos += data.len();
    }

    pub fn deserialize<T: SerializeSafe>(&mut self) -> T {
        T::deserialize_self(self)
    }

    pub fn drain_fd(&mut self, idx: usize) -> OwnedFd {
        self.fds[idx].take().unwrap()
    }
}

pub trait SerializeSafe {
    fn serialize_self(&self, s: &mut Serializer);
    fn deserialize_self(d: &mut Deserializer) -> Self;
}
