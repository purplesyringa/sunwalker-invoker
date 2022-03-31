#![feature(async_closure, map_try_insert, unix_chown)]

mod image {
    pub(crate) mod config;
    pub(crate) mod language;
    pub(crate) mod mount;
    pub(crate) mod package;
    pub(crate) mod sandbox;
}

mod cgroups;

mod client;

mod config;

mod corepool;

pub mod init;

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

mod system;

mod worker;
