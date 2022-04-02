use anyhow::Result;
use lisp;
use lisp::{evaluate, parse, LispType, State, Term};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Deserialize, LispType, Serialize)]
#[lisp(name = "config")]
pub struct Config {
    pub packages: HashMap<String, Package>,
}

#[derive(Clone, Debug, Deserialize, LispType, Serialize)]
#[lisp(name = "package")]
pub struct Package {
    pub languages: HashMap<String, Language>,
}

#[derive(Clone, Debug, Deserialize, LispType, Serialize)]
#[lisp(name = "language")]
pub struct Language {
    pub identify: Term,
    pub base_rule: Term,
    pub inputs: Vec<String>,
    pub build: Term,
    pub run: RunStatement,
}

#[derive(Clone, Debug, Deserialize, LispType, Serialize)]
#[lisp(name = "run")]
pub struct RunStatement {
    pub prerequisites: Term,
    pub argv: Term,
}

impl Config {
    pub fn load(config: &str) -> Result<Config> {
        Ok(evaluate(parse(config)?, &State::new())?.to_native()?)
    }
}
