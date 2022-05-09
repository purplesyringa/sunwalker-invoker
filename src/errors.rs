use multiprocessing::Object;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, Object)]
pub enum Error {
    InvokerFailure(String),
    ConductorFailure(String),
    CommunicationError(String),
    UserFailure(String),
}

pub use Error::*;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}
