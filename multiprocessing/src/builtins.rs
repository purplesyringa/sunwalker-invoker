use crate::{Deserializer, SerializeSafe, Serializer};

impl SerializeSafe for bool {
    fn serialize_self(&self, s: &mut Serializer) {
        s.serialize(&(*self as u8));
    }
    fn deserialize_self(d: &mut Deserializer) -> Self {
        d.deserialize::<u8>() != 0
    }
}

impl SerializeSafe for char {
    fn serialize_self(&self, s: &mut Serializer) {
        s.serialize(&(*self as u32))
    }
    fn deserialize_self(d: &mut Deserializer) -> Self {
        char::from_u32(d.deserialize::<u32>()).unwrap()
    }
}

impl SerializeSafe for String {
    fn serialize_self(&self, s: &mut Serializer) {
        // XXX: unnecessary heap usage
        s.serialize(&Vec::from(self.as_bytes()))
    }
    fn deserialize_self(d: &mut Deserializer) -> Self {
        // XXX: unnecessary heap usage
        std::str::from_utf8(&d.deserialize::<Vec<u8>>())
            .unwrap()
            .to_string()
    }
}

impl SerializeSafe for std::ffi::CString {
    fn serialize_self(&self, s: &mut Serializer) {
        // XXX: unnecessary heap usage
        s.serialize(&Vec::from(self.as_bytes()))
    }
    fn deserialize_self(d: &mut Deserializer) -> Self {
        // XXX: unnecessary heap usage
        Self::new(std::str::from_utf8(&d.deserialize::<Vec<u8>>()).unwrap()).unwrap()
    }
}

impl SerializeSafe for std::ffi::OsString {
    fn serialize_self(&self, s: &mut Serializer) {
        // XXX: unnecessary heap usage
        use std::os::unix::ffi::OsStringExt;
        s.serialize(&self.clone().into_vec())
    }
    fn deserialize_self(d: &mut Deserializer) -> Self {
        use std::os::unix::ffi::OsStringExt;
        Self::from_vec(d.deserialize())
    }
}

impl SerializeSafe for () {
    fn serialize_self(&self, _s: &mut Serializer) {}
    fn deserialize_self(_d: &mut Deserializer) -> Self {
        ()
    }
}

impl<T: SerializeSafe, U: SerializeSafe> SerializeSafe for (T, U) {
    fn serialize_self(&self, s: &mut Serializer) {
        s.serialize(&self.0);
        s.serialize(&self.1);
    }
    fn deserialize_self(d: &mut Deserializer) -> Self {
        let a = d.deserialize();
        let b = d.deserialize();
        (a, b)
    }
}

impl<T> SerializeSafe for std::marker::PhantomData<T> {
    fn serialize_self(&self, _s: &mut Serializer) {}
    fn deserialize_self(_d: &mut Deserializer) -> Self {
        Self {}
    }
}

impl<T: SerializeSafe> SerializeSafe for Option<T> {
    fn serialize_self(&self, s: &mut Serializer) {
        match self {
            None => s.serialize(&false),
            Some(ref x) => {
                s.serialize(&true);
                s.serialize(x);
            }
        }
    }
    fn deserialize_self(d: &mut Deserializer) -> Self {
        if d.deserialize::<bool>() {
            Some(d.deserialize())
        } else {
            None
        }
    }
}

// TODO: compress multiple Rc's to the same location
impl<T: SerializeSafe> SerializeSafe for std::rc::Rc<T> {
    fn serialize_self(&self, s: &mut Serializer) {
        s.serialize(&**self);
    }
    fn deserialize_self(d: &mut Deserializer) -> Self {
        Self::new(d.deserialize())
    }
}

// TODO: same here
impl<T: SerializeSafe> SerializeSafe for std::sync::Arc<T> {
    fn serialize_self(&self, s: &mut Serializer) {
        s.serialize(&**self);
    }
    fn deserialize_self(d: &mut Deserializer) -> Self {
        Self::new(d.deserialize())
    }
}

impl SerializeSafe for std::path::PathBuf {
    fn serialize_self(&self, s: &mut Serializer) {
        // XXX: unnecessary heap usage
        s.serialize(&self.as_os_str().to_owned());
    }
    fn deserialize_self(d: &mut Deserializer) -> Self {
        d.deserialize::<std::ffi::OsString>().into()
    }
}

macro_rules! impl_serializesafe_for_primitive {
    ($t:ty) => {
        impl SerializeSafe for $t {
            fn serialize_self(&self, s: &mut Serializer) {
                s.write(&self.to_ne_bytes());
            }
            fn deserialize_self(d: &mut Deserializer) -> Self {
                let mut buf = [0u8; std::mem::size_of::<Self>()];
                d.read(&mut buf);
                Self::from_ne_bytes(buf)
            }
        }
    };
}

impl_serializesafe_for_primitive!(i8);
impl_serializesafe_for_primitive!(i16);
impl_serializesafe_for_primitive!(i32);
impl_serializesafe_for_primitive!(i64);
impl_serializesafe_for_primitive!(i128);
impl_serializesafe_for_primitive!(isize);
impl_serializesafe_for_primitive!(u8);
impl_serializesafe_for_primitive!(u16);
impl_serializesafe_for_primitive!(u32);
impl_serializesafe_for_primitive!(u64);
impl_serializesafe_for_primitive!(u128);
impl_serializesafe_for_primitive!(usize);
impl_serializesafe_for_primitive!(f32);
impl_serializesafe_for_primitive!(f64);

macro_rules! impl_serializesafe_for_sequence {
    (
        $ty:ident < T $(: $tbound1:ident $(+ $tbound2:ident)*)* $(, $typaram:ident : $bound1:ident $(+ $bound2:ident)*)* >,
        $seq:ident,
        $size:ident,
        $with_capacity:expr,
        $push:expr
    ) => {
        impl<T: SerializeSafe $(+ $tbound1 $(+ $tbound2)*)* $(, $typaram: $bound1 $(+ $bound2)*,)*> SerializeSafe
            for $ty<T $(, $typaram)*>
        {
            fn serialize_self(&self, s: &mut Serializer) {
                s.serialize(&self.len());
                for item in self.iter() {
                    s.serialize(item);
                }
            }
            fn deserialize_self(d: &mut Deserializer) -> Self {
                let $size: usize = d.deserialize();
                let mut $seq = $with_capacity;
                for _ in 0..$size {
                    $push(&mut $seq, d.deserialize());
                }
                $seq
            }
        }
    }
}

macro_rules! impl_serializesafe_for_map {
    (
        $ty:ident <
            K $(: $kbound1:ident $(+ $kbound2:ident)*)*,
            V
            $(, $typaram:ident : $bound1:ident $(+ $bound2:ident)*)*
        >,
        $size:ident,
        $with_capacity:expr
    ) => {
        impl<
            K: SerializeSafe $(+ $kbound1 $(+ $kbound2)*)*,
            V: SerializeSafe
            $(, $typaram: $bound1 $(+ $bound2)*,)*
        > SerializeSafe
            for $ty<K, V $(, $typaram)*>
        {
            fn serialize_self(&self, s: &mut Serializer) {
                s.serialize(&self.len());
                for (key, value) in self.iter() {
                    s.serialize(key);
                    s.serialize(value);
                }
            }
            fn deserialize_self(d: &mut Deserializer) -> Self {
                let $size: usize = d.deserialize();
                let mut map = $with_capacity;
                for _ in 0..$size {
                    map.insert(d.deserialize(), d.deserialize());
                }
                map
            }
        }
    }
}

use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque};
use std::hash::{BuildHasher, Hash};
impl_serializesafe_for_sequence!(Vec<T>, seq, size, Vec::with_capacity(size), Vec::push);
impl_serializesafe_for_sequence!(
    BinaryHeap<T: Ord>,
    seq,
    size,
    BinaryHeap::with_capacity(size),
    BinaryHeap::push
);
impl_serializesafe_for_sequence!(
    BTreeSet<T: Eq + Ord>,
    seq,
    size,
    BTreeSet::new(),
    BTreeSet::insert
);
impl_serializesafe_for_sequence!(
    LinkedList<T>,
    seq,
    size,
    LinkedList::new(),
    LinkedList::push_back
);
impl_serializesafe_for_sequence!(
    HashSet<T: Eq + Hash, S: BuildHasher + Default>,
    seq,
    size,
    HashSet::with_capacity_and_hasher(size, S::default()),
    HashSet::insert
);
impl_serializesafe_for_sequence!(
    VecDeque<T>,
    seq,
    size,
    VecDeque::with_capacity(size),
    VecDeque::push_back
);
impl_serializesafe_for_map!(BTreeMap<K: Ord, V>, size, BTreeMap::new());
impl_serializesafe_for_map!(HashMap<K: Eq + Hash, V, S: BuildHasher + Default>, size, HashMap::with_capacity_and_hasher(size, S::default()));

impl<T: SerializeSafe, E: SerializeSafe> SerializeSafe for Result<T, E> {
    fn serialize_self(&self, s: &mut Serializer) {
        match self {
            Ok(ref ok) => {
                s.serialize(&true);
                s.serialize(ok);
            }
            Err(ref err) => {
                s.serialize(&false);
                s.serialize(err);
            }
        }
    }
    fn deserialize_self(d: &mut Deserializer) -> Self {
        if d.deserialize::<bool>() {
            Ok(d.deserialize())
        } else {
            Err(d.deserialize())
        }
    }
}

impl SerializeSafe for std::fs::File {
    fn serialize_self(&self, s: &mut Serializer) {
        use std::os::unix::io::AsRawFd;
        let fd = s.add_fd(self.as_raw_fd());
        s.serialize(&fd)
    }
    fn deserialize_self(d: &mut Deserializer) -> Self {
        let fd = d.deserialize();
        <Self as From<std::os::unix::io::OwnedFd>>::from(d.drain_fd(fd))
    }
}
