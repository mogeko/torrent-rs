use crate::error::{Error, ErrorKind};

use super::{Bencode, Bytes};

/// Decode a single bencoded value from the beginning of `data`.
///
/// Uses a recursive-descent parser. Returns the decoded [`Bencode`] value
/// and the remaining unconsumed bytes.
///
/// # Errors
///
/// Returns [`Error`](crate::error::Error) with kind:
/// - [`BencodeInvalidSyntax`](crate::error::ErrorKind::BencodeInvalidSyntax) —
///   data doesn't start with a valid bencode type marker (`0-9`, `i`, `l`, `d`).
/// - [`BencodeUnexpectedEof`](crate::error::ErrorKind::BencodeUnexpectedEof) —
///   input ends before the value is complete.
/// - [`BencodeInvalidInteger`](crate::error::ErrorKind::BencodeInvalidInteger) —
///   malformed integer (leading zeros, negative zero, empty digit string).
/// - [`BencodeIntegerOverflow`](crate::error::ErrorKind::BencodeIntegerOverflow) —
///   integer value exceeds `i64::MIN..=i64::MAX`.
///
/// # Examples
///
/// ```
/// use torrent_core::bencode::decode;
///
/// let (val, rest) = decode(b"4:spam").unwrap();
/// assert!(rest.is_empty());
/// ```
///
/// Decoding a nested dictionary:
///
/// ```
/// use torrent_core::bencode::{decode, Bencode, Bytes};
///
/// let (val, rest) = decode(b"d3:fooi42e3:bar4:spame").unwrap();
/// assert!(rest.is_empty());
/// ```
pub fn decode(data: &[u8]) -> Result<(Bencode, &[u8]), Error> {
    tracing::debug!("decoding bencode ({} bytes)", data.len());
    if data.is_empty() {
        return Err(Error::new(ErrorKind::BencodeUnexpectedEof));
    }
    match data[0] {
        b'0'..=b'9' => parse_string(data),
        b'i' => parse_integer(data),
        b'l' => parse_list(data),
        b'd' => parse_dict(data),
        _ => Err(Error::new(ErrorKind::BencodeInvalidSyntax)),
    }
}

/// Parse a bencoded byte string.
///
/// Format: `<decimal-length>:<bytes>`
fn parse_string(data: &[u8]) -> Result<(Bencode, &[u8]), Error> {
    let colon_pos = data
        .iter()
        .position(|&b| b == b':')
        .ok_or(Error::new(ErrorKind::BencodeInvalidSyntax))?;

    let len_str = &data[..colon_pos];
    let len_u64 = parse_decimal_u64(len_str)?;
    let len: usize =
        usize::try_from(len_u64).map_err(|_| Error::new(ErrorKind::BencodeIntegerOverflow))?;

    // No leading zeros allowed (but "0" alone is fine)
    if len_str.len() > 1 && len_str[0] == b'0' {
        return Err(Error::new(ErrorKind::BencodeInvalidInteger));
    }

    let start = colon_pos + 1;
    let end = start + len;
    if end > data.len() {
        return Err(Error::new(ErrorKind::BencodeUnexpectedEof));
    }

    let bytes = Bytes::copy_from_slice(&data[start..end]);
    Ok((Bencode::Bytes(bytes), &data[end..]))
}

/// Parse a bencoded integer.
///
/// Format: `i<integer>e`
fn parse_integer(data: &[u8]) -> Result<(Bencode, &[u8]), Error> {
    // data[0] is 'i' — find the closing 'e'
    let end = data
        .iter()
        .position(|&b| b == b'e')
        .ok_or(Error::new(ErrorKind::BencodeUnexpectedEof))?;

    let num_str = &data[1..end]; // skip 'i'

    if num_str.is_empty() {
        return Err(Error::new(ErrorKind::BencodeInvalidInteger));
    }

    // Disallow leading zeros
    let has_sign = num_str[0] == b'-';
    let digits_start = if has_sign { 1 } else { 0 };

    if digits_start >= num_str.len() {
        return Err(Error::new(ErrorKind::BencodeInvalidInteger));
    }

    // "i-0e" (negative zero) is not allowed
    if has_sign && num_str[1] == b'0' && num_str.len() == 2 {
        return Err(Error::new(ErrorKind::BencodeInvalidInteger));
    }

    // Leading zero check: more than one digit and first digit is '0'
    if num_str.len() - digits_start > 1 && num_str[digits_start] == b'0' {
        return Err(Error::new(ErrorKind::BencodeInvalidInteger));
    }

    let num_str_ascii =
        std::str::from_utf8(num_str).map_err(|_| Error::new(ErrorKind::BencodeInvalidInteger))?;

    let value: i64 = num_str_ascii
        .parse()
        .map_err(|_| Error::new(ErrorKind::BencodeIntegerOverflow))?;

    Ok((Bencode::Integer(value), &data[end + 1..]))
}

/// Parse a bencoded list.
///
/// Format: `l<elements>e`
fn parse_list(data: &[u8]) -> Result<(Bencode, &[u8]), Error> {
    let mut rest = &data[1..]; // skip 'l'
    let mut items = Vec::new();

    loop {
        if rest.is_empty() {
            return Err(Error::new(ErrorKind::BencodeUnexpectedEof));
        }
        if rest[0] == b'e' {
            return Ok((Bencode::List(items), &rest[1..]));
        }
        let (item, remaining) = decode(rest)?;
        items.push(item);
        rest = remaining;
    }
}

/// Parse a bencoded dictionary.
///
/// Format: `d<key><value>...e`
///
/// Keys must be bencoded byte strings. Duplicate keys are treated as an error.
fn parse_dict(data: &[u8]) -> Result<(Bencode, &[u8]), Error> {
    let mut rest = &data[1..]; // skip 'd'
    let mut entries: Vec<(Bytes, Bencode)> = Vec::new();

    loop {
        if rest.is_empty() {
            return Err(Error::new(ErrorKind::BencodeUnexpectedEof));
        }
        if rest[0] == b'e' {
            // Sort entries by key (BEP 3 requires lexicographic order).
            // This ensures that encode ∘ decode is idempotent.
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            return Ok((Bencode::Dict(entries), &rest[1..]));
        }
        // Key must be a byte string
        let (key_val, remaining) = decode(rest)?;
        let key = match key_val {
            Bencode::Bytes(b) => b,
            _ => return Err(Error::new(ErrorKind::BencodeInvalidSyntax)),
        };

        // Check that there are bytes remaining after the key for the value
        if remaining.is_empty() {
            return Err(Error::new(ErrorKind::BencodeUnexpectedEof));
        }

        let (val, remaining) = decode(remaining)?;

        // Reject duplicate keys (BEP 3 requires unique dict keys).
        if entries.iter().any(|(k, _)| k == &key) {
            return Err(Error::new(ErrorKind::BencodeInvalidSyntax));
        }

        entries.push((key, val));
        rest = remaining;
    }
}

/// Parse a decimal string into a `u64`.
///
/// This only accepts ASCII digits (no sign).
fn parse_decimal_u64(s: &[u8]) -> Result<u64, Error> {
    if s.is_empty() {
        return Err(Error::new(ErrorKind::BencodeInvalidInteger));
    }
    let mut val: u64 = 0;
    for &b in s {
        if !b.is_ascii_digit() {
            return Err(Error::new(ErrorKind::BencodeInvalidInteger));
        }
        val = val
            .checked_mul(10)
            .and_then(|v| v.checked_add((b - b'0') as u64))
            .ok_or(Error::new(ErrorKind::BencodeIntegerOverflow))?;
    }
    Ok(val)
}

#[cfg(test)]
mod decode_tests {
    use super::*;

    // ── Strings ────────────────────────────────────────────────────────

    #[test]
    fn decode_string() {
        let (val, rest) = decode(b"4:spam").unwrap();
        assert_eq!(val, Bencode::Bytes(Bytes::from("spam")));
        assert!(rest.is_empty());
    }

    #[test]
    fn decode_string_empty() {
        let (val, rest) = decode(b"0:").unwrap();
        assert_eq!(val, Bencode::Bytes(Bytes::new()));
        assert!(rest.is_empty());
    }

    #[test]
    fn decode_string_with_zeros() {
        let data = [b'5', b':', b'h', b'e', b'l', b'l', b'o'];
        let (val, rest) = decode(&data).unwrap();
        assert_eq!(val, Bencode::Bytes(Bytes::from("hello")));
        assert!(rest.is_empty());
    }

    #[test]
    fn decode_string_binary() {
        let raw = [b'4', b':', 0x00, 0xFF, 0xAB, 0xCD];
        let (val, rest) = decode(&raw).unwrap();
        assert_eq!(
            val,
            Bencode::Bytes(Bytes::from(&[0x00, 0xFF, 0xAB, 0xCD][..]))
        );
        assert!(rest.is_empty());
    }

    // ── Integers ───────────────────────────────────────────────────────

    #[test]
    fn decode_integer_positive() {
        let (val, rest) = decode(b"i42e").unwrap();
        assert_eq!(val, Bencode::Integer(42));
        assert!(rest.is_empty());
    }

    #[test]
    fn decode_integer_negative() {
        let (val, rest) = decode(b"i-3e").unwrap();
        assert_eq!(val, Bencode::Integer(-3));
        assert!(rest.is_empty());
    }

    #[test]
    fn decode_integer_zero() {
        let (val, rest) = decode(b"i0e").unwrap();
        assert_eq!(val, Bencode::Integer(0));
        assert!(rest.is_empty());
    }

    #[test]
    fn decode_integer_large() {
        let input = format!("i{}e", i64::MAX);
        let (val, rest) = decode(input.as_bytes()).unwrap();
        assert_eq!(val, Bencode::Integer(i64::MAX));
        assert!(rest.is_empty());
    }

    #[test]
    fn decode_integer_negative_large() {
        let input = format!("i{}e", i64::MIN);
        let (val, rest) = decode(input.as_bytes()).unwrap();
        assert_eq!(val, Bencode::Integer(i64::MIN));
        assert!(rest.is_empty());
    }

    // ── Integer error cases ────────────────────────────────────────────

    #[test]
    fn decode_integer_leading_zero_rejected() {
        assert!(decode(b"i01e").is_err());
    }

    #[test]
    fn decode_integer_negative_zero_rejected() {
        assert!(decode(b"i-0e").is_err());
    }

    #[test]
    fn decode_integer_truncated() {
        assert!(decode(b"i42").is_err()); // missing 'e'
    }

    #[test]
    fn decode_integer_empty() {
        assert!(decode(b"ie").is_err());
    }

    #[test]
    fn decode_integer_overflow_rejected() {
        let big = format!("i{}0e", i64::MAX); // larger than i64::MAX
        let result = decode(big.as_bytes());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().kind(),
            crate::error::ErrorKind::BencodeIntegerOverflow
        );
    }

    // ── Lists ──────────────────────────────────────────────────────────

    #[test]
    fn decode_list() {
        let (val, rest) = decode(b"l4:spami42ee").unwrap();
        assert_eq!(
            val,
            Bencode::List(vec![
                Bencode::Bytes(Bytes::from("spam")),
                Bencode::Integer(42),
            ])
        );
        assert!(rest.is_empty());
    }

    #[test]
    fn decode_list_empty() {
        let (val, rest) = decode(b"le").unwrap();
        assert_eq!(val, Bencode::List(vec![]));
        assert!(rest.is_empty());
    }

    #[test]
    fn decode_list_nested() {
        let (val, rest) = decode(b"ll4:spami42eee").unwrap();
        assert_eq!(
            val,
            Bencode::List(vec![Bencode::List(vec![
                Bencode::Bytes(Bytes::from("spam")),
                Bencode::Integer(42),
            ])])
        );
        assert!(rest.is_empty());
    }

    // ── Dictionaries ───────────────────────────────────────────────────

    #[test]
    fn decode_dict() {
        let (val, rest) = decode(b"d3:bar4:spam3:fooi42ee").unwrap();
        assert_eq!(
            val,
            Bencode::Dict(vec![
                (Bytes::from("bar"), Bencode::Bytes(Bytes::from("spam"))),
                (Bytes::from("foo"), Bencode::Integer(42)),
            ])
        );
        assert!(rest.is_empty());
    }

    #[test]
    fn decode_dict_empty() {
        let (val, rest) = decode(b"de").unwrap();
        assert_eq!(val, Bencode::Dict(vec![]));
        assert!(rest.is_empty());
    }

    #[test]
    fn decode_dict_nested() {
        let data = b"d4:listl5:itemsi3eee";
        let (val, rest) = decode(data).unwrap();
        match val {
            Bencode::Dict(ref entries) => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].0, Bytes::from("list"));
                assert_eq!(
                    entries[0].1,
                    Bencode::List(vec![
                        Bencode::Bytes(Bytes::from("items")),
                        Bencode::Integer(3),
                    ])
                );
            }
            _ => panic!("expected Dict"),
        }
        assert!(rest.is_empty());
    }

    // ── Error cases ────────────────────────────────────────────────────

    #[test]
    fn decode_empty_input() {
        assert!(decode(b"").is_err());
    }

    #[test]
    fn decode_truncated_string() {
        assert!(decode(b"5:ab").is_err()); // declares 5 bytes, only 2 available
    }

    #[test]
    fn decode_truncated_list() {
        assert!(decode(b"l4:spam").is_err()); // missing 'e'
    }

    #[test]
    fn decode_invalid_syntax() {
        assert!(decode(b"x").is_err());
    }

    #[test]
    fn decode_partial_consumption() {
        let (val, rest) = decode(b"i1ei2e").unwrap();
        assert_eq!(val, Bencode::Integer(1));
        assert_eq!(rest, b"i2e");
    }

    // ── Dict with non-string key ───────────────────────────────────────

    #[test]
    fn decode_dict_non_string_key_rejected() {
        // Dictionary with an integer key (invalid)
        assert!(decode(b"di42e4:spame").is_err());
    }
}
