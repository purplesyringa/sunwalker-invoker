use crate::term::{CallTerm, Error, Term};
use lazy_static::lazy_static;
use std::any::Any;
use std::collections::HashMap;
use std::sync::RwLock;

lazy_static! {
    pub static ref USER_FNS_HASHMAP: RwLock<HashMap<&'static str, fn(CallTerm) -> Result<Box<dyn Any>, Error>>> =
        RwLock::new(HashMap::new());
}

trait TryFromIfDefined<T> {
    fn try_from(value: T) -> Result<Self, Error>
    where
        Self: Sized;
}

impl<T, U> TryFromIfDefined<T> for U {
    default fn try_from(_: T) -> Result<U, Error> {
        Err(Error {
            message: format!("Type U cannot be converted from T"),
        })
    }
}

impl<T, U: From<T>> TryFromIfDefined<T> for U {
    fn try_from(value: T) -> Result<U, Error> {
        Ok(value.into())
    }
}

struct Evaluator<T> {
    data: std::marker::PhantomData<T>,
}

trait Evaluate<T> {
    fn evaluate(term: Term) -> Result<T, Error>;
}

impl<T: 'static> Evaluate<T> for Evaluator<T> {
    default fn evaluate(term: Term) -> Result<T, Error> {
        match term {
            Term::Call(call) => match &USER_FNS_HASHMAP.read().unwrap().get(call.name.as_str()) {
                Some(ref func) => {
                    let name = call.name.clone();
                    Ok(*func(call)?.downcast::<T>().map_err(|_| Error {
                        message: format!("{} returns wrong return type", name),
                    })?)
                }
                None => crate::builtins::evaluate_call_builtin(call),
            },
            Term::String(s) => <T as TryFromIfDefined<String>>::try_from(s),
            Term::Number(num) => <T as TryFromIfDefined<i64>>::try_from(num),
            Term::Nil => unimplemented!(),
        }
    }
}

impl Evaluate<Term> for Evaluator<Term> {
    fn evaluate(term: Term) -> Result<Term, Error> {
        Ok(term)
    }
}

pub fn evaluate<T: 'static>(term: Term) -> Result<T, Error> {
    Evaluator::<T>::evaluate(term)
}
