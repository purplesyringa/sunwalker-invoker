use crate::term::Error;
use core::ffi::c_void;
use std::any::Any;
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

struct TypedValue {
    ty: Rc<Type>,
    value: Box<dyn Any>,
}

#[derive(Clone)]
pub struct TypedRef {
    value: Rc<TypedValue>,
}

impl TypedRef {
    pub fn new<T: LispType + 'static>(value: T) -> TypedRef {
        TypedRef {
            value: Rc::new(TypedValue {
                ty: Rc::new(Type {
                    ty_id: std::any::TypeId::of::<T>(),
                    ty_str: <T as TyStrTrait>::ty_str,
                    eq: <T as EqTrait>::eq,
                    hash: <T as HashTrait>::hash,
                    add: <T as AddTrait>::add,
                    debug: <T as DebugTrait>::debug,
                }),
                value: Box::new(value),
            }),
        }
    }

    pub fn to_concrete<T: LispType + 'static>(&self) -> Result<T, Error> {
        Ok((*self.value.value.downcast_ref::<T>().ok_or_else(|| Error {
            message: format!(
                "Expected instance of type {}, got {:?}",
                <T as TyStrTrait>::ty_str(),
                self,
            ),
        })?)
        .clone())
    }

    pub fn to_native<T: NativeType + 'static>(&self) -> Result<T, Error> {
        T::from_lisp_ref(&self)
    }

    pub fn try_add(&self, other: &TypedRef) -> Result<TypedRef, Error> {
        if self.value.ty.ty_id == other.value.ty.ty_id {
            (self.value.ty.add)(self.value.value.as_ref(), other.value.value.as_ref())
        } else {
            Err(Error {
                message: format!(
                    "Cannot add instances of distinct types {} and {}",
                    (self.value.ty.ty_str)(),
                    (other.value.ty.ty_str)(),
                ),
            })
        }
    }
}

pub trait LispType: Clone {
    fn get_type_name() -> String;
}

pub type List = Vec<TypedRef>;
pub type Map = HashMap<TypedRef, TypedRef>;
pub type Pair = (TypedRef, TypedRef);

impl LispType for List {
    fn get_type_name() -> String {
        "list".to_string()
    }
}
impl LispType for Map {
    fn get_type_name() -> String {
        "map".to_string()
    }
}
impl LispType for Pair {
    fn get_type_name() -> String {
        "pair".to_string()
    }
}
impl LispType for String {
    fn get_type_name() -> String {
        "string".to_string()
    }
}
impl LispType for i64 {
    fn get_type_name() -> String {
        "int".to_string()
    }
}

pub struct Type {
    ty_id: std::any::TypeId,
    ty_str: fn() -> String,
    eq: fn(&dyn Any, &dyn Any) -> bool,
    hash: fn(&dyn Any, &mut DefaultHasher),
    add: fn(&dyn Any, &dyn Any) -> Result<TypedRef, Error>,
    debug: fn(&dyn Any) -> String,
}

trait TyStrTrait {
    fn ty_str() -> String;
}
trait EqTrait {
    fn eq(a: &dyn Any, b: &dyn Any) -> bool;
}
trait HashTrait {
    fn hash(value: &dyn Any, state: &mut DefaultHasher);
}
trait AddTrait {
    fn add(a: &dyn Any, b: &dyn Any) -> Result<TypedRef, Error>;
}
trait DebugTrait {
    fn debug(a: &dyn Any) -> String;
}

impl<T> TyStrTrait for T {
    default fn ty_str() -> String {
        std::any::type_name::<T>().to_string()
    }
}
impl<T> EqTrait for T {
    default fn eq(a: &dyn Any, b: &dyn Any) -> bool {
        std::ptr::eq(a, b)
    }
}
impl<T> HashTrait for T {
    default fn hash(value: &dyn Any, state: &mut DefaultHasher) {
        (value as *const dyn Any as *const c_void).hash(state);
    }
}
impl<T> AddTrait for T {
    default fn add(_: &dyn Any, _: &dyn Any) -> Result<TypedRef, Error> {
        Err(Error {
            message: format!("Type {} does not implement Add", T::ty_str()).to_string(),
        })
    }
}
impl<T> DebugTrait for T {
    default fn debug(_: &dyn Any) -> String {
        format!("{} {{ .. }}", Self::ty_str())
    }
}

impl<T: LispType + 'static> TyStrTrait for T {
    fn ty_str() -> String {
        T::get_type_name()
    }
}
impl<T: LispType + Eq + 'static> EqTrait for T {
    fn eq(a: &dyn Any, b: &dyn Any) -> bool {
        let a = a.downcast_ref::<T>().unwrap();
        let b = b.downcast_ref::<T>().unwrap();
        a == b
    }
}
impl<T: LispType + Hash + 'static> HashTrait for T {
    fn hash(value: &dyn Any, state: &mut DefaultHasher) {
        value.downcast_ref::<T>().unwrap().hash(state);
    }
}
impl AddTrait for String {
    fn add(a: &dyn Any, b: &dyn Any) -> Result<TypedRef, Error> {
        let a = a.downcast_ref::<Self>().unwrap();
        let b = b.downcast_ref::<Self>().unwrap();
        Ok(TypedRef::new(a.clone() + b))
    }
}
impl AddTrait for i64 {
    fn add(a: &dyn Any, b: &dyn Any) -> Result<TypedRef, Error> {
        let a = a.downcast_ref::<Self>().unwrap();
        let b = b.downcast_ref::<Self>().unwrap();
        Ok(TypedRef::new(a + b))
    }
}
impl AddTrait for List {
    fn add(a: &dyn Any, b: &dyn Any) -> Result<TypedRef, Error> {
        let mut a = a.downcast_ref::<Self>().unwrap().clone();
        let b = b.downcast_ref::<Self>().unwrap().clone();
        a.extend(b);
        Ok(TypedRef::new(a))
    }
}
impl AddTrait for Map {
    fn add(a: &dyn Any, b: &dyn Any) -> Result<TypedRef, Error> {
        let mut a = a.downcast_ref::<Self>().unwrap().clone();
        let b = b.downcast_ref::<Self>().unwrap().clone();
        a.extend(b);
        Ok(TypedRef::new(a))
    }
}
impl<T: LispType + Debug + 'static> DebugTrait for T {
    default fn debug(a: &dyn Any) -> String {
        format!("{:?}", a.downcast_ref::<Self>().unwrap())
    }
}

impl PartialEq for TypedRef {
    fn eq(&self, other: &Self) -> bool {
        self.value.ty.ty_id == other.value.ty.ty_id
            && (self.value.ty.eq)(self.value.value.as_ref(), other.value.value.as_ref())
    }
}

impl Eq for TypedRef {}

impl Hash for TypedRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut def_state = DefaultHasher::new();
        (self.value.ty.hash)(self.value.value.as_ref(), &mut def_state);
        state.write_u64(def_state.finish());
    }
}

impl Debug for TypedRef {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        fmt.write_str(&(self.value.ty.debug)(self.value.value.as_ref()))
    }
}

pub trait NativeType: Clone {
    fn from_lisp_ref(value: &TypedRef) -> Result<Self, Error>
    where
        Self: Sized;
}

impl<T: NativeType + 'static> NativeType for Vec<T> {
    fn from_lisp_ref(value: &TypedRef) -> Result<Self, Error> {
        let mut vec = Vec::new();
        for item in value.to_concrete::<List>()?.into_iter() {
            vec.push(item.to_native::<T>()?);
        }
        Ok(vec)
    }
}

impl<K: NativeType + Eq + Hash + 'static, V: NativeType + 'static> NativeType for HashMap<K, V> {
    fn from_lisp_ref(value: &TypedRef) -> Result<Self, Error> {
        let mut map = HashMap::new();
        for (key, value) in value.to_concrete::<Map>()?.into_iter() {
            map.insert(key.to_native()?, value.to_native()?);
        }
        Ok(map)
    }
}

impl<T: NativeType + 'static, U: NativeType + 'static> NativeType for (T, U) {
    fn from_lisp_ref(value: &TypedRef) -> Result<Self, Error> {
        let (a, b) = value.to_concrete::<Pair>()?;
        Ok((a.to_native()?, b.to_native()?))
    }
}

impl NativeType for String {
    fn from_lisp_ref(value: &TypedRef) -> Result<Self, Error> {
        value.to_concrete::<String>()
    }
}

impl NativeType for i64 {
    fn from_lisp_ref(value: &TypedRef) -> Result<Self, Error> {
        value.to_concrete::<i64>()
    }
}

impl NativeType for TypedRef {
    fn from_lisp_ref(value: &TypedRef) -> Result<Self, Error> {
        Ok((*value).clone())
    }
}
