use std::convert::Infallible;
use std::fmt::Debug;

pub struct CallTerm {
    pub name: String,
    pub params: Vec<Term>,
}

impl Debug for CallTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}", self.name)?;
        for param in &self.params {
            write!(f, " {:?}", param)?;
        }
        write!(f, ")")
    }
}

pub enum Term {
    Call(CallTerm),
    String(String),
    Number(i64),
    Nil,
}

impl Debug for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Term::Call(call) => write!(f, "{:?}", call),
            Term::String(s) => write!(f, "{:?}", s),
            Term::Number(num) => write!(f, "{}", num),
            Term::Nil => write!(f, "nil"),
        }
    }
}

#[derive(Debug)]
pub struct Error {
    pub message: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for Error {}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        Error {
            message: "Infallible".to_owned(),
        }
    }
}
