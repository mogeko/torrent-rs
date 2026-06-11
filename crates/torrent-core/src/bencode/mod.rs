//! Bencode encoding and decoding (BEP 3).
//!
//! Bencode is the serialization format used throughout the BitTorrent protocol
//! — in `.torrent` files, tracker responses, and DHT messages. This module
//! provides a recursive-descent parser and an encoder with strict validation.
//!
//! # Key Types
//!
//! - [`Bencode`] — the AST representing any bencode value (string, integer, list, dict)
//! - [`decode`] — parse bencode bytes into a [`Bencode`] value
//! - [`encode`] — serialize a [`Bencode`] value back to bytes
//!
//! # Examples
//!
//! ```
//! use torrent_core::bencode::{decode, encode, Bencode};
//!
//! // Decode
//! let (val, rest) = decode(b"4:spam").unwrap();
//! assert!(rest.is_empty());
//!
//! // Encode
//! let encoded = encode(&val);
//! assert_eq!(encoded, b"4:spam");
//! ```

mod decode;
mod encode;
mod util;

pub use self::decode::decode;
pub use self::encode::encode;
pub use self::util::*;

use std::fmt;

use bytes::Bytes;

/// Represents a bencoded value as defined in BEP 3.
///
/// Bencode is the encoding used by BitTorrent for `.torrent` files, tracker
/// responses, and DHT messages. It supports four types: byte strings,
/// integers, lists, and dictionaries.
///
/// Dictionaries store entries as a `Vec<(Bytes, Bencode)>` rather than
/// a `HashMap` to preserve the BEP 3-required lexicographic key ordering.
///
/// # Examples
///
/// ```
/// use torrent_core::bencode::Bencode;
/// use bytes::Bytes;
///
/// // String
/// let s = Bencode::Bytes(Bytes::from("hello"));
///
/// // Integer
/// let n = Bencode::Integer(42);
///
/// // List
/// let list = Bencode::List(vec![
///     Bencode::Bytes(Bytes::from("a")),
///     Bencode::Integer(1),
/// ]);
///
/// // Dictionary (keys sorted lexicographically)
/// let dict = Bencode::Dict(vec![
///     (Bytes::from("bar"), Bencode::Integer(2)),
///     (Bytes::from("foo"), Bencode::Integer(1)),
/// ]);
/// ```
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
