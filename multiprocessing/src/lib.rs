#![feature(io_safety)]
// The code using the following features is mostly copied from stdlib, so it should stay relatively
// safe
#![feature(auto_traits)]
#![feature(negative_impls)]

extern crate self as multiprocessing;

pub use multiprocessing_derive::*;

pub trait Entrypoint {}

pub mod imp;

pub mod soundness;
pub use soundness::*;

pub mod serde;
pub use crate::serde::*;

pub mod ipc;
pub use ipc::{channel, duplex, Duplex, Receiver, Sender};

pub mod subprocess;
pub use subprocess::Child;

pub mod builtins;
