use crate::term::{CallTerm, Error, Term};
use crate::{evaluate, State, TypedRef};
use itertools::Itertools;
use lisp_derive::function;
use std::collections::HashMap;

pub fn as_item1(call: CallTerm) -> Result<Term, Error> {
    match call.params.into_iter().collect_tuple() {
        Some((t,)) => Ok(t),
        None => Err(Error {
            message: format!("Exactly 1 argument required"),
        }),
    }
}

pub fn as_tuple2(call: CallTerm) -> Result<(Term, Term), Error> {
    match call.params.into_iter().collect_tuple() {
        Some(t) => Ok(t),
        None => Err(Error {
            message: format!("Exactly 2 arguments required"),
        }),
    }
}

#[function]
fn list(call: CallTerm, state: &State) -> Result<TypedRef, Error> {
    let mut vec = Vec::new();
    for param in call.params.into_iter() {
        vec.push(evaluate(param, state)?);
    }
    Ok(TypedRef::new(vec))
}

#[function]
fn pair(call: CallTerm, state: &State) -> Result<TypedRef, Error> {
    let (a, b) = as_tuple2(call)?;
    Ok(TypedRef::new((evaluate(a, state)?, evaluate(b, state)?)))
}

#[function]
fn map(call: CallTerm, state: &State) -> Result<TypedRef, Error> {
    let vec: Vec<(TypedRef, TypedRef)> = evaluate(as_item1(call)?, state)?.to_native()?;
    Ok(TypedRef::new(HashMap::from_iter(vec.into_iter())))
}

#[function]
fn var(call: CallTerm, state: &State) -> Result<TypedRef, Error> {
    let name: String = evaluate(as_item1(call)?, state)?.to_concrete()?;
    state.get_var(&name)
}

#[function]
fn quote(call: CallTerm, _: &State) -> Result<TypedRef, Error> {
    let quoted: Term = as_item1(call)?;
    Ok(TypedRef::new(quoted))
}

#[function]
fn concat(call: CallTerm, state: &State) -> Result<TypedRef, Error> {
    if call.params.is_empty() {
        return Err(Error {
            message: "(concat) cannot be called without arguments".to_string(),
        });
    }
    let mut it = call.params.into_iter();
    let mut result = evaluate(it.next().unwrap(), state)?;
    for param in it {
        result = result.try_add(&evaluate(param, state)?)?;
    }
    Ok(result)
}

// We need this dummy function due to https://github.com/rust-lang/rust/issues/47384
pub fn initialize() {}
