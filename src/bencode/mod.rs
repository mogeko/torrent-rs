mod decode;
mod encode;
mod util;

use std::fmt;

use bytes::Bytes;

/// Represents a bencoded value as defined in BEP 3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bencode {
    /// Byte string: `<length>:<contents>`
    Bytes(Bytes),
    /// Integer: `i<integer>e`
    Integer(i64),
    /// List: `l<values>e`
    List(Vec<Bencode>),
    /// Dictionary: `d<key><value>...e`
    ///
    /// Keys are bencoded byte strings. Entries are stored in a `Vec` to
    /// preserve the bencode-required lexicographic key ordering.
    Dict(Vec<(Bytes, Bencode)>),
}

impl From<&[u8]> for Bencode {
    fn from(value: &[u8]) -> Self {
        Bencode::Bytes(Bytes::copy_from_slice(value))
    }
}

impl From<&str> for Bencode {
    fn from(value: &str) -> Self {
        Bencode::Bytes(Bytes::copy_from_slice(value.as_bytes()))
    }
}

impl From<String> for Bencode {
    fn from(value: String) -> Self {
        Bencode::Bytes(Bytes::from(value))
    }
}

impl From<i64> for Bencode {
    fn from(value: i64) -> Self {
        Bencode::Integer(value)
    }
}

impl<T: Into<Bencode>> From<Vec<T>> for Bencode {
    fn from(value: Vec<T>) -> Self {
        Bencode::List(value.into_iter().map(Into::into).collect())
    }
}

impl fmt::Display for Bencode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Bencode::Bytes(b) => write!(f, "b{:?}", b),
            Bencode::Integer(i) => write!(f, "i{}", i),
            Bencode::List(items) => {
                write!(f, "[")?;
                for (i, v) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
            Bencode::Dict(entries) => {
                write!(f, "{{")?;
                for (i, (k, v)) in entries.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}: {}", k, v)?;
                }
                write!(f, "}}")
            }
        }
    }
}

pub use decode::decode;
pub use encode::encode;
pub use util::*;
