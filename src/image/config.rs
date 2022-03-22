use anyhow::Result;
use lisp;
use lisp::{evaluate, parse, Term, TryFromTerm};
use std::collections::HashMap;

#[derive(TryFromTerm, Debug)]
#[lisp(name = "config")]
pub struct Config {
    package: HashMap<String, Package>,
}

#[derive(TryFromTerm, Debug)]
#[lisp(name = "package")]
pub struct Package {
    languages: HashMap<String, Language>,
}

#[derive(TryFromTerm, Debug)]
#[lisp(name = "language")]
pub struct Language {
    identify: Term,
    base_rule: Term,
    inputs: Vec<String>,
    build: Term,
    run: RunStatement,
}

#[derive(TryFromTerm, Debug)]
#[lisp(name = "run")]
pub struct RunStatement {
    prerequisites: Term,
    argv: Term,
}

impl Config {
    pub fn load(config: &str) -> Result<Config> {
        Ok(evaluate(parse(config)?)?)
    }
}
