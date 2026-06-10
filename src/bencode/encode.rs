use bytes::Bytes;

use crate::bencode::Bencode;

/// Encode a `Bencode` value into its bencoded byte representation.
pub fn encode(val: &Bencode) -> Vec<u8> {
    match val {
        Bencode::Bytes(b) => encode_bytes(b),
        Bencode::Integer(i) => encode_integer(*i),
        Bencode::List(items) => encode_list(items),
        Bencode::Dict(entries) => encode_dict(entries),
    }
}

fn encode_bytes(b: &Bytes) -> Vec<u8> {
    let len_str = b.len().to_string();
    let mut out = Vec::with_capacity(len_str.len() + 1 + b.len());
    out.extend_from_slice(len_str.as_bytes());
    out.push(b':');
    out.extend_from_slice(b);
    out
}

fn encode_integer(i: i64) -> Vec<u8> {
    let s = i.to_string();
    let mut out = Vec::with_capacity(s.len() + 2);
    out.push(b'i');
    out.extend_from_slice(s.as_bytes());
    out.push(b'e');
    out
}

fn encode_list(items: &[Bencode]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'l');
    for item in items {
        out.extend_from_slice(&encode(item));
    }
    out.push(b'e');
    out
}

fn encode_dict(entries: &[(Bytes, Bencode)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'd');

    // BEP 3 requires dictionary keys to be sorted lexicographically.
    let mut sorted: Vec<_> = entries.iter().collect();
    sorted.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (key, val) in &sorted {
        out.extend_from_slice(&encode(&Bencode::Bytes((*key).clone())));
        out.extend_from_slice(&encode(val));
    }
    out.push(b'e');
    out
}

#[cfg(test)]
mod encode_tests {
    use super::*;
    use crate::bencode::Bencode;
    use bytes::Bytes;

    // ── Strings ────────────────────────────────────────────────────────

    #[test]
    fn encode_string() {
        let val = Bencode::Bytes(Bytes::from("spam"));
        assert_eq!(encode(&val), b"4:spam");
    }

    #[test]
    fn encode_string_empty() {
        let val = Bencode::Bytes(Bytes::new());
        assert_eq!(encode(&val), b"0:");
    }

    // ── Integers ───────────────────────────────────────────────────────

    #[test]
    fn encode_integer_positive() {
        let val = Bencode::Integer(42);
        assert_eq!(encode(&val), b"i42e");
    }

    #[test]
    fn encode_integer_negative() {
        let val = Bencode::Integer(-3);
        assert_eq!(encode(&val), b"i-3e");
    }

    #[test]
    fn encode_integer_zero() {
        let val = Bencode::Integer(0);
        assert_eq!(encode(&val), b"i0e");
    }

    #[test]
    fn encode_integer_large() {
        let val = Bencode::Integer(i64::MAX);
        let expected = format!("i{}e", i64::MAX);
        assert_eq!(encode(&val), expected.as_bytes());
    }

    #[test]
    fn encode_integer_negative_large() {
        let val = Bencode::Integer(i64::MIN);
        let expected = format!("i{}e", i64::MIN);
        assert_eq!(encode(&val), expected.as_bytes());
    }

    // ── Lists ──────────────────────────────────────────────────────────

    #[test]
    fn encode_list() {
        let val = Bencode::List(vec![
            Bencode::Bytes(Bytes::from("spam")),
            Bencode::Integer(42),
        ]);
        assert_eq!(encode(&val), b"l4:spami42ee");
    }

    #[test]
    fn encode_list_empty() {
        let val = Bencode::List(vec![]);
        assert_eq!(encode(&val), b"le");
    }

    #[test]
    fn encode_list_nested() {
        let val = Bencode::List(vec![Bencode::List(vec![
            Bencode::Bytes(Bytes::from("spam")),
            Bencode::Integer(42),
        ])]);
        assert_eq!(encode(&val), b"ll4:spami42eee");
    }

    // ── Dictionaries ───────────────────────────────────────────────────

    #[test]
    fn encode_dict() {
        let val = Bencode::Dict(vec![
            (Bytes::from("bar"), Bencode::Bytes(Bytes::from("spam"))),
            (Bytes::from("foo"), Bencode::Integer(42)),
        ]);
        assert_eq!(encode(&val), b"d3:bar4:spam3:fooi42ee");
    }

    #[test]
    fn encode_dict_empty() {
        let val = Bencode::Dict(vec![]);
        assert_eq!(encode(&val), b"de");
    }

    // ── Round-trip ─────────────────────────────────────────────────────

    #[test]
    fn roundtrip_string() {
        let original = Bencode::Bytes(Bytes::from("hello bencode"));
        let encoded = encode(&original);
        let (decoded, _) = super::super::decode::decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn roundtrip_integer() {
        let original = Bencode::Integer(i64::MAX);
        let encoded = encode(&original);
        let (decoded, _) = super::super::decode::decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn roundtrip_complex() {
        // Keys must be sorted lexicographically so that decode result matches.
        let original = Bencode::Dict(vec![
            (Bytes::from("length"), Bencode::Integer(1024)),
            (Bytes::from("name"), Bencode::Bytes(Bytes::from("test.txt"))),
            (Bytes::from("piece length"), Bencode::Integer(256)),
            (
                Bytes::from("pieces"),
                Bencode::Bytes(Bytes::from("abcdefghijklmnopqrst")),
            ),
        ]);
        let encoded = encode(&original);
        let (decoded, _) = super::super::decode::decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }
}
