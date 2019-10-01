use crate::AnyError;
use postgres_protocol::{types, IsNull, Oid};

pub trait Serializable: Send + Sync {
    fn serialize(&self, buf: &mut Vec<u8>) -> IsNull;
}

pub trait HasOid {
    fn oid() -> Oid;
    fn array_oid() -> Oid;
}

pub trait Deserializable: Sized {
    fn deserialize(buf: &[u8]) -> Result<Self, AnyError>;
}

macro_rules! trivial_impl {
    ($typ:ty, $oid:expr, $arr:expr, $ser:path, $des:path) => {
        impl Serializable for $typ {
            fn serialize(&self, buf: &mut Vec<u8>) -> IsNull {
                $ser(*self, buf);

                IsNull::No
            }
        }

        impl HasOid for $typ {
            fn oid() -> Oid {
                $oid
            }

            fn array_oid() -> Oid {
                $arr
            }
        }

        impl Deserializable for $typ {
            fn deserialize(buf: &[u8]) -> Result<Self, AnyError> {
                $des(buf)
            }
        }
    };

    ($typ:ty, $oid: expr, $arr: expr, $ser:path) => {
        impl Serializable for $typ {
            fn serialize(&self, buf: &mut Vec<u8>) -> IsNull {
                $ser(self, buf);

                IsNull::No
            }
        }

        impl HasOid for $typ {
            fn oid() -> Oid {
                $oid
            }

            fn array_oid() -> Oid {
                $arr
            }
        }
    };
}

trivial_impl!(bool, 16, 1000, types::bool_to_sql, types::bool_from_sql);
trivial_impl!(i8, 18, 1002, types::char_to_sql, types::char_from_sql);
trivial_impl!(f32, 700, 1021, types::float4_to_sql, types::float4_from_sql);
trivial_impl!(f64, 701, 1022, types::float8_to_sql, types::float8_from_sql);
trivial_impl!(i16, 21, 1005, types::int2_to_sql, types::int2_from_sql);
trivial_impl!(i32, 23, 1007, types::int4_to_sql, types::int4_from_sql);
trivial_impl!(i64, 20, 1016, types::int8_to_sql, types::int8_from_sql);

trivial_impl!(str, 25, 1009, types::text_to_sql);
trivial_impl!(String, 25, 1009, types::text_to_sql);

impl Deserializable for String {
    fn deserialize(buf: &[u8]) -> Result<Self, AnyError> {
        types::text_from_sql(buf).map(str::to_string)
    }
}

impl<T: HasOid + Serializable> Serializable for [T] {
    fn serialize(&self, buf: &mut Vec<u8>) -> IsNull {
        types::array_to_sql(
            Some(types::ArrayDimension {
                len: self.len() as i32,
                lower_bound: 0,
            }),
            T::oid(),
            self,
            |item, buf| Ok(item.serialize(buf)),
            buf,
        )
        .unwrap();

        IsNull::No
    }
}

impl<T: HasOid + Serializable> Serializable for Vec<T> {
    fn serialize(&self, buf: &mut Vec<u8>) -> IsNull {
        types::array_to_sql(
            Some(types::ArrayDimension {
                len: self.len() as i32,
                lower_bound: 0,
            }),
            T::oid(),
            self,
            |item, buf| Ok(item.serialize(buf)),
            buf,
        )
        .unwrap();

        IsNull::No
    }
}

impl<T: Serializable> Serializable for Option<T> {
    fn serialize(&self, buf: &mut Vec<u8>) -> IsNull {
        match self {
            Some(val) => val.serialize(buf),
            None => IsNull::Yes,
        }
    }
}

impl<T: HasOid> HasOid for Option<T> {
    fn oid() -> Oid {
        T::oid()
    }

    fn array_oid() -> Oid {
        T::array_oid()
    }
}

impl<T: Serializable> Serializable for &T {
    fn serialize(&self, buf: &mut Vec<u8>) -> IsNull {
        T::serialize(self, buf)
    }
}

impl<T: HasOid> HasOid for &T {
    fn oid() -> Oid {
        T::oid()
    }

    fn array_oid() -> Oid {
        T::array_oid()
    }
}
