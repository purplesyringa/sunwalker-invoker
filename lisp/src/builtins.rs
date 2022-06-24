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

pub fn as_tuple3(call: CallTerm) -> Result<(Term, Term, Term), Error> {
    match call.params.into_iter().collect_tuple() {
        Some(t) => Ok(t),
        None => Err(Error {
            message: format!("Exactly 3 arguments required"),
        }),
    }
}

pub fn as_tuple4(call: CallTerm) -> Result<(Term, Term, Term, Term), Error> {
    match call.params.into_iter().collect_tuple() {
        Some(t) => Ok(t),
        None => Err(Error {
            message: format!("Exactly 4 arguments required"),
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

#[function]
fn sed(call: CallTerm, state: &State) -> Result<TypedRef, Error> {
    let (re, repl, s) = as_tuple3(call)?;
    let mut re: String = evaluate(re, state)?.to_native()?;
    let mut repl: String = evaluate(repl, state)?.to_native()?;
    let s: String = evaluate(s, state)?.to_native()?;

    // A bit of compatibility with sed regular expressions
    re = re
        .replace("\\(", "<re match>")
        .replace("(", "\\(")
        .replace("<re match>", "(");
    re = re
        .replace("\\)", "<re match>")
        .replace(")", "\\)")
        .replace("<re match>", ")");
    for d in 1..=9 {
        repl = repl.replace(&format!("\\{}", d), &format!("${{{}}}", d));
    }

    Ok(TypedRef::new(
        regex::Regex::new(&re)
            .map_err(|e| lisp::Error {
                message: format!("(sed) failed: {}", e),
            })?
            .replace(&s, &repl)
            .to_string(),
    ))
}

#[function]
fn head(call: CallTerm, state: &State) -> Result<TypedRef, Error> {
    let (n_lines, s) = as_tuple2(call)?;
    let n_lines: i64 = evaluate(n_lines, state)?.to_native()?;
    let s: String = evaluate(s, state)?.to_native()?;

    Ok(TypedRef::new(
        s.split("\n")
            .take(n_lines.try_into().map_err(|_| Error {
                message: "n_lines does not fit".to_string(),
            })?)
            .map(|s| s.to_string() + "\n")
            .join(""),
    ))
}

#[function]
fn stripnl(call: CallTerm, state: &State) -> Result<TypedRef, Error> {
    let s: String = evaluate(as_item1(call)?, state)?.to_native()?;
    Ok(TypedRef::new(
        s.strip_suffix("\n").unwrap_or(&s).to_string(),
    ))
}

// We need this dummy function due to https://github.com/rust-lang/rust/issues/47384
pub fn initialize() {}
