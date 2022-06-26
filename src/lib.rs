#![feature(async_closure, map_try_insert, unix_chown, io_safety, try_blocks)]

mod image {
    pub(crate) mod config;
    pub(crate) mod image;
    pub(crate) mod language;
    pub(crate) mod mount;
    pub(crate) mod package;
    pub(crate) mod program;
    pub(crate) mod sandbox;
    pub(crate) mod strategy;
}

mod cgroups;

mod client;

mod config;

mod errors;

pub mod init;

mod message {
    pub(crate) mod c2i;
    pub(crate) mod i2c;
}

mod problem {
    pub(crate) mod problem;
    pub(crate) mod store;
    pub(crate) mod verdict;
}

mod submission;

mod system;

mod worker;
