use torrent_core::bencode::{Bencode, Bytes, decode, encode};

/// Test that each known-good test vector can be decoded and re-encoded
/// to produce the same bytes.
macro_rules! roundtrip_test_vector {
    ($name:ident, $file:expr) => {
        #[test]
        fn $name() {
            let data = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/", $file));
            let (val, rest) = decode(data).unwrap();
            assert!(rest.is_empty(), "expected full consumption of test vector");
            let re_encoded = encode(&val);
            assert_eq!(re_encoded, data, "re-encode must exactly match input");
        }
    };
}

roundtrip_test_vector!(tv_string, "string.bin");
roundtrip_test_vector!(tv_integer_positive, "integer_positive.bin");
roundtrip_test_vector!(tv_integer_negative, "integer_negative.bin");
roundtrip_test_vector!(tv_integer_zero, "integer_zero.bin");
roundtrip_test_vector!(tv_list, "list.bin");
roundtrip_test_vector!(tv_list_empty, "list_empty.bin");
roundtrip_test_vector!(tv_dict, "dict.bin");
roundtrip_test_vector!(tv_dict_empty, "dict_empty.bin");
roundtrip_test_vector!(tv_dict_nested, "dict_nested.bin");

/// Test that invalid test vectors are rejected.
macro_rules! reject_test_vector {
    ($name:ident, $file:expr) => {
        #[test]
        fn $name() {
            let data = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/", $file));
            assert!(decode(data).is_err(), "expected decode failure");
        }
    };
}

reject_test_vector!(tv_integer_negative_zero, "integer_negative_zero.bin");
reject_test_vector!(tv_integer_leading_zero, "integer_leading_zero.bin");

#[test]
fn decode_realistic_metainfo_dict() {
    // A simplified metainfo-like structure to verify real-world usage
    // All dict keys must be in lexicographic sorted order.
    let bencode = Bencode::Dict(vec![
        (
            Bytes::from("announce"),
            Bencode::Bytes(Bytes::from("http://tracker.example.com/announce")),
        ),
        (Bytes::from("creation date"), Bencode::Integer(1712345678)),
        (
            Bytes::from("info"),
            Bencode::Dict(vec![
                (Bytes::from("length"), Bencode::Integer(1073741824)),
                (
                    Bytes::from("name"),
                    Bencode::Bytes(Bytes::from("ubuntu-24.04.iso")),
                ),
                (Bytes::from("piece length"), Bencode::Integer(262144)),
                (
                    Bytes::from("pieces"),
                    Bencode::Bytes(Bytes::from(
                        "aaaaaaaaaaaaaaaaaaaa\
                         bbbbbbbbbbbbbbbbbbbb\
                         cccccccccccccccccccc",
                    )),
                ),
            ]),
        ),
    ]);

    let encoded = encode(&bencode);
    let (decoded, rest) = decode(&encoded).unwrap();
    assert!(rest.is_empty());
    assert_eq!(bencode, decoded);
}
