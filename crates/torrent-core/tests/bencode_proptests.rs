use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence};

use torrent_core::bencode::{Bencode, Bytes, decode, encode};

fn proptest_config() -> ProptestConfig {
    ProptestConfig {
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "tests/proptest-regressions/bencode_proptests.txt",
        ))),
        ..ProptestConfig::default()
    }
}

/// Strategy that generates arbitrary `Bencode` values.
fn bencode_strategy() -> impl Strategy<Value = Bencode> {
    let leaf = prop_oneof![
        // Byte strings (0-64 bytes of arbitrary bytes)
        prop::collection::vec(any::<u8>(), 0..64).prop_map(|v| Bencode::Bytes(Bytes::from(v))),
        // Integers
        any::<i64>().prop_map(Bencode::Integer),
    ];

    leaf.prop_recursive(
        4,   // max recursion depth
        256, // max total nodes
        10,  // max items per container
        |inner| {
            prop_oneof![
                // Lists
                prop::collection::vec(inner.clone(), 0..10).prop_map(Bencode::List),
                // Dictionaries (keys are sorted to match decode ordering)
                prop::collection::vec(
                    (
                        prop::collection::vec(any::<u8>(), 0..16).prop_map(Bytes::from),
                        inner,
                    ),
                    0..8,
                )
                .prop_map(|mut entries: Vec<(Bytes, Bencode)>| {
                    // Sort by key (BEP 3 lexicographic order) and deduplicate
                    entries.sort_by(|a, b| a.0.cmp(&b.0));
                    entries.dedup_by(|a, b| a.0 == b.0);
                    Bencode::Dict(entries)
                }),
            ]
        },
    )
}

#[test]
fn encode_decode_roundtrip() {
    proptest!(proptest_config(), |(val in bencode_strategy())| {
        let encoded = encode(&val);
        let (decoded, rest) = decode(&encoded).unwrap();
        prop_assert_eq!(val, decoded);
        prop_assert!(rest.is_empty());
    });
}

#[test]
fn encoded_bytes_are_well_formed() {
    proptest!(proptest_config(), |(val in bencode_strategy())| {
        let encoded = encode(&val);
        if encoded.is_empty() {
            return Ok(());
        }
        match encoded[0] {
            b'0'..=b'9' => {
                prop_assert!(
                    encoded.contains(&b':'),
                    "string encoding must contain ':'"
                );
            }
            b'i' => {
                prop_assert_eq!(encoded.last(), Some(&b'e'), "integer must end with 'e'");
            }
            b'l' => {
                prop_assert_eq!(encoded.last(), Some(&b'e'), "list must end with 'e'");
            }
            b'd' => {
                prop_assert_eq!(encoded.last(), Some(&b'e'), "dict must end with 'e'");
            }
            other => {
                panic!("invalid bencode start byte: {:#04x}", other);
            }
        }
    });
}
