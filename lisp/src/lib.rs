#![feature(specialization)]

mod parse;
pub use parse::parse;

pub mod term;
pub use term::{Error, Term};

pub use lisp_derive::TryFromTerm;

pub mod builtins;

pub use ctor::ctor;

pub mod evaluate;
pub use evaluate::evaluate;
