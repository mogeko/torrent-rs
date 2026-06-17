use crate::error::{Error, ErrorKind};

use super::{Bencode, Bytes, decode, encode};

/// Decode a bencoded byte string directly to a `String`.
///
/// Convenience wrapper around [`decode`]
/// that extracts a byte string and converts it to UTF-8.
///
/// # Errors
///
/// Returns an error if:
/// - The value is not a byte string (e.g., an integer, list, or dict)
/// - The byte string contains invalid UTF-8
///
/// # Examples
///
/// ```
/// use torrent_core::bencode::decode_str;
///
/// let s = decode_str(b"5:hello").unwrap();
/// assert_eq!(s, "hello");
/// ```
pub fn decode_str(data: &[u8]) -> Result<String, Error> {
    let (val, _rest) = decode(data)?;
    match val {
        Bencode::Bytes(b) => {
            String::from_utf8(b.to_vec()).map_err(|_| Error::new(ErrorKind::InvalidInput))
        }
        _ => Err(Error::new(ErrorKind::InvalidInput)),
    }
}

/// Encode a `&str` as a bencoded byte string.
///
/// Convenience wrapper that encodes a Rust string as a bencoded byte string
/// (length-prefixed format).
///
/// # Examples
///
/// ```
/// use torrent_core::bencode::encode_str;
///
/// assert_eq!(encode_str("spam"), b"4:spam");
/// ```
pub fn encode_str(s: &str) -> Vec<u8> {
    encode(&Bencode::Bytes(Bytes::copy_from_slice(s.as_bytes())))
}

/// Get a value by key from a bencoded dictionary.
///
/// Returns `None` if `val` is not a `Dict` or the key is not found.
/// Uses a linear scan — acceptable for the small dictionaries typical
/// of bencoded tracker responses and torrent files.
///
/// # Examples
///
/// ```
/// use torrent_core::bencode::{Bencode, Bytes, dict_get};
///
/// let dict = Bencode::Dict(vec![
///     (Bytes::from("foo"), Bencode::Integer(42)),
/// ]);
///
/// assert_eq!(dict_get(&dict, b"foo"), Some(&Bencode::Integer(42)));
/// assert_eq!(dict_get(&dict, b"missing"), None);
/// ```
pub fn dict_get<'a>(val: &'a Bencode, key: &[u8]) -> Option<&'a Bencode> {
    match val {
        Bencode::Dict(entries) => {
            // Linear scan — acceptable for small dicts typical of bencode
            entries
                .iter()
                .find(|(k, _)| k.as_ref() == key)
                .map(|(_, v)| v)
        }
        _ => None,
    }
}

/// Convenience: get an integer from a dict by key.
///
/// Returns `None` if the key is missing or the value is not an integer.
///
/// # Examples
///
/// ```
/// use torrent_core::bencode::{Bencode, Bytes, dict_get_int};
///
/// let dict = Bencode::Dict(vec![
///     (Bytes::from("count"), Bencode::Integer(42)),
/// ]);
///
/// assert_eq!(dict_get_int(&dict, b"count"), Some(42));
/// assert_eq!(dict_get_int(&dict, b"missing"), None);
/// ```
pub fn dict_get_int(val: &Bencode, key: &[u8]) -> Option<i64> {
    match dict_get(val, key)? {
        Bencode::Integer(i) => Some(*i),
        _ => None,
    }
}

/// Convenience: get a byte string from a dict by key.
///
/// Returns `None` if the key is missing or the value is not a byte string.
///
/// # Examples
///
/// ```
/// use torrent_core::bencode::{Bencode, Bytes, dict_get_bytes};
///
/// let dict = Bencode::Dict(vec![
///     (Bytes::from("name"), Bencode::Bytes(Bytes::from("hello"))),
/// ]);
///
/// assert_eq!(dict_get_bytes(&dict, b"name").map(|b| &b[..]), Some(&b"hello"[..]));
/// assert_eq!(dict_get_bytes(&dict, b"missing"), None);
/// ```
pub fn dict_get_bytes<'a>(val: &'a Bencode, key: &[u8]) -> Option<&'a Bytes> {
    match dict_get(val, key)? {
        Bencode::Bytes(b) => Some(b),
        _ => None,
    }
}

#[cfg(test)]
mod util_tests {
    use super::*;

    #[test]
    fn test_decode_str() {
        let result = decode_str(b"5:hello").unwrap();
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_decode_str_invalid_utf8() {
        // Invalid UTF-8 byte sequence
        assert!(decode_str(b"2:\xFF\xFE").is_err());
    }

    #[test]
    fn test_decode_str_not_a_string() {
        assert!(decode_str(b"i42e").is_err());
    }

    #[test]
    fn test_encode_str() {
        assert_eq!(encode_str("spam"), b"4:spam");
    }

    #[test]
    fn test_dict_get() {
        let dict = Bencode::Dict(vec![
            (Bytes::from("foo"), Bencode::Integer(42)),
            (Bytes::from("bar"), Bencode::Bytes(Bytes::from("baz"))),
        ]);

        assert_eq!(dict_get(&dict, b"foo"), Some(&Bencode::Integer(42)));
        assert_eq!(
            dict_get(&dict, b"bar"),
            Some(&Bencode::Bytes(Bytes::from("baz")))
        );
        assert_eq!(dict_get(&dict, b"missing"), None);
    }

    #[test]
    fn test_dict_get_not_dict() {
        let val = Bencode::Integer(42);
        assert_eq!(dict_get(&val, b"foo"), None);
    }

    #[test]
    fn test_dict_get_int() {
        let dict = Bencode::Dict(vec![(Bytes::from("count"), Bencode::Integer(7))]);

        assert_eq!(dict_get_int(&dict, b"count"), Some(7));
        assert_eq!(dict_get_int(&dict, b"missing"), None);
    }

    #[test]
    fn test_dict_get_bytes() {
        let dict = Bencode::Dict(vec![(
            Bytes::from("name"),
            Bencode::Bytes(Bytes::from("test.txt")),
        )]);

        assert_eq!(
            dict_get_bytes(&dict, b"name"),
            Some(&Bytes::from("test.txt"))
        );
        assert_eq!(dict_get_bytes(&dict, b"missing"), None);
    }
}
