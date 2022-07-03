use multiprocessing::Object;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, Object)]
pub enum Error {
    InvokerFailure(String),
    ConductorFailure(String),
    ConfigurationFailure(String),
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

pub trait ToError {
    fn context_invoker(self, message: &str) -> Error;
    fn with_context_invoker<F: FnOnce() -> String>(self, f: F) -> Error;
}

impl<E: std::fmt::Display> ToError for E {
    fn context_invoker(self, message: &str) -> Error {
        Error::InvokerFailure(format!("{}: {}", message, self))
    }
    fn with_context_invoker<F: FnOnce() -> String>(self, f: F) -> Error {
        Error::InvokerFailure(format!("{}: {}", f(), self))
    }
}

pub trait ToResult<T> {
    fn context_invoker(self, message: &str) -> Result<T, Error>;
    fn with_context_invoker<F: FnOnce() -> String>(self, f: F) -> Result<T, Error>;
}

impl<T, E: std::fmt::Display> ToResult<T> for Result<T, E> {
    fn context_invoker(self, message: &str) -> Result<T, Error> {
        self.map_err(|e| e.context_invoker(message))
    }
    fn with_context_invoker<F: FnOnce() -> String>(self, f: F) -> Result<T, Error> {
        self.map_err(|e| e.with_context_invoker(f))
    }
}

impl<T> ToResult<T> for Option<T> {
    fn context_invoker(self, message: &str) -> Result<T, Error> {
        self.ok_or_else(|| Error::InvokerFailure(message.to_string()))
    }
    fn with_context_invoker<F: FnOnce() -> String>(self, f: F) -> Result<T, Error> {
        self.ok_or_else(|| Error::InvokerFailure(f()))
    }
}
