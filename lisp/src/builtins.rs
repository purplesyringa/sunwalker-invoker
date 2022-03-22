use crate::evaluate;
use crate::term::{CallTerm, Error, Term};
use itertools::Itertools;
use lisp_derive::generic_functions;
use std::collections::HashMap;

fn as_item1(call: CallTerm) -> Result<Term, Error> {
    match call.params.into_iter().collect_tuple() {
        Some((t,)) => Ok(t),
        None => Err(Error {
            message: format!("Exactly 1 argument required"),
        }),
    }
}

fn as_tuple2(call: CallTerm) -> Result<(Term, Term), Error> {
    match call.params.into_iter().collect_tuple() {
        Some(t) => Ok(t),
        None => Err(Error {
            message: format!("Exactly 2 arguments required"),
        }),
    }
}

generic_functions! {{
    fn list<T: 'static>(t: CallTerm) -> Vec<T> {
        let mut vec = Vec::new();
        for param in t.params.into_iter() {
            vec.push(evaluate(param)?);
        }
        Ok(vec)
    }

    fn pair<T: 'static, U: 'static>(t: CallTerm) -> (T, U) {
        let (a, b) = as_tuple2(t)?;
        Ok((evaluate(a)?, evaluate(b)?))
    }

    fn map<K: 'static + Eq + std::hash::Hash, V: 'static>(t: CallTerm) -> HashMap<K, V> {
        let vec = as_item1(t)?;
        Ok(evaluate::<Vec<(K, V)>>(vec)?.into_iter().collect())
    }
}}
