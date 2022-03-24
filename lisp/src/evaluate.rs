use crate::term::{CallTerm, Error, Term};
use crate::typed::TypedRef;
use crate::State;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::RwLock;

lazy_static! {
    pub static ref USER_FNS_HASHMAP: RwLock<HashMap<&'static str, fn(CallTerm, &State) -> Result<TypedRef, Error>>> =
        RwLock::new(HashMap::new());
}

pub fn evaluate(term: Term, state: &State) -> Result<TypedRef, Error> {
    match term {
        Term::Call(call) => match &USER_FNS_HASHMAP.read().unwrap().get(call.name.as_str()) {
            Some(ref func) => func(call, state),
            None => Err(Error {
                message: format!("Function {} is not defined", call.name),
            }),
        },
        Term::String(s) => Ok(TypedRef::new(s)),
        Term::Number(num) => Ok(TypedRef::new(num)),
        Term::Nil => unimplemented!(),
    }
}
