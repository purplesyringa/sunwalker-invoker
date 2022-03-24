use crate::term::Error;
use crate::typed::{LispType, TypedRef};
use std::collections::HashMap;
use std::result::Result;

pub struct State {
    pub vars: HashMap<String, TypedRef>,
}

impl State {
    pub fn new() -> State {
        State {
            vars: HashMap::new(),
        }
    }

    pub fn set_var<T: LispType + 'static>(&mut self, name: String, value: T) -> &mut State {
        self.vars.insert(name, TypedRef::new(value));
        self
    }

    pub fn get_var(&self, name: &str) -> Result<TypedRef, Error> {
        Ok(self
            .vars
            .get(name)
            .ok_or_else(|| Error {
                message: format!("Variable {} is not defined", name),
            })?
            .clone())
    }
}
