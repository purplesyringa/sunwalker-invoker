#![feature(async_closure, map_try_insert, unix_chown)]

mod image {
    pub(crate) mod config;
    pub(crate) mod image;
    pub(crate) mod language;
    pub(crate) mod mount;
    pub(crate) mod package;
    pub(crate) mod program;
    pub(crate) mod sandbox;
}

mod cgroups;

mod client;

mod config;

mod corepool;

mod errors;

pub mod init;

mod ipc;

mod message {
    pub(crate) mod c2i;
    pub(crate) mod i2c;
}

mod problem {
    pub(crate) mod dependencies;
    pub(crate) mod problem;
    pub(crate) mod store;
}

mod process;

mod submission;

mod system;

mod worker;
