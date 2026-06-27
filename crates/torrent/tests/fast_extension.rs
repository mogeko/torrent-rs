//! Integration tests for BEP 6 Fast Extension.
//!
//! Protocol-level roundtrip and encode/decode tests live in
//! `crates/torrent-core/src/peer/message.rs` unit tests.
//! These tests focus on cross-module integration and the
//! torrent crate's re-exports.

use std::net::SocketAddr;

use torrent::peer::{Handshake, PeerMessage, compute_allowed_fast_set, decode, encode};

// ── Re-exports ──

#[test]
fn compute_allowed_fast_set_is_re_exported() {
    let addr: SocketAddr = "10.0.0.1:6881".parse().unwrap();
    let set = compute_allowed_fast_set(&[0xAB; 20], addr, 50, 5);
    assert_eq!(set.len(), 5);
}

// ── Handshake bit 44 ──

#[test]
fn handshake_sets_fast_extension_bit() {
    let hs = Handshake::with_extensions([1u8; 20], [2u8; 20], &[44]);
    assert!(hs.has_extension(44));
    // Byte 5, bit 3 = 0x08
    assert_eq!(hs.reserved[5] & 0x08, 0x08);
}

#[test]
fn handshake_without_fast_extension_does_not_set_bit_44() {
    let hs = Handshake::new([1u8; 20], [2u8; 20]);
    assert!(!hs.has_extension(44));
    assert_eq!(hs.reserved[5] & 0x08, 0);
}

// ── BEP 6 message encode/decode via torrent re-exports ──

#[test]
fn roundtrip_haveall() {
    let msg = PeerMessage::HaveAll;
    let encoded = encode(&msg);
    let decoded = decode(&encoded).unwrap();
    assert_eq!(msg, decoded);
    assert_eq!(encoded, vec![0, 0, 0, 1, 14]);
}

#[test]
fn roundtrip_havenone() {
    let msg = PeerMessage::HaveNone;
    let encoded = encode(&msg);
    let decoded = decode(&encoded).unwrap();
    assert_eq!(msg, decoded);
    assert_eq!(encoded, vec![0, 0, 0, 1, 15]);
}

#[test]
fn roundtrip_suggest() {
    let msg = PeerMessage::Suggest(42);
    let encoded = encode(&msg);
    let decoded = decode(&encoded).unwrap();
    assert_eq!(msg, decoded);
}

#[test]
fn roundtrip_allowed_fast() {
    let msg = PeerMessage::AllowedFast(7);
    let encoded = encode(&msg);
    let decoded = decode(&encoded).unwrap();
    assert_eq!(msg, decoded);
}

#[test]
fn roundtrip_reject() {
    let msg = PeerMessage::Reject {
        index: 1,
        begin: 2048,
        length: 16384,
    };
    let encoded = encode(&msg);
    let decoded = decode(&encoded).unwrap();
    assert_eq!(msg, decoded);
}

#[test]
fn unknown_message_does_not_error() {
    // BEP 3: unknown message IDs must be accepted for forward compatibility.
    let data = [0, 0, 0, 2, 99, 0x42];
    let decoded = decode(&data).unwrap();
    assert_eq!(
        decoded,
        PeerMessage::Unknown {
            id: 99,
            data: vec![0x42],
        }
    );
}

// ── Allowed Fast set properties ──

#[test]
fn allowed_fast_set_is_deterministic() {
    let info_hash = [0x42u8; 20];
    let addr: SocketAddr = "192.168.1.1:6881".parse().unwrap();
    let a = compute_allowed_fast_set(&info_hash, addr, 100, 10);
    let b = compute_allowed_fast_set(&info_hash, addr, 100, 10);
    assert_eq!(a, b);
}

#[test]
fn allowed_fast_set_bounds_check() {
    let addr: SocketAddr = "10.0.0.1:9999".parse().unwrap();
    let set = compute_allowed_fast_set(&[0u8; 20], addr, 8, 10);
    // All indices must be < 8
    for &idx in &set {
        assert!(idx < 8, "index {} out of bounds", idx);
    }
    // At most 8 unique indices
    assert!(set.len() <= 8);
}

#[test]
fn allowed_fast_set_ipv4_vs_ipv6_differ() {
    let info_hash = [0x7Fu8; 20];
    let v4: SocketAddr = "1.1.1.1:6881".parse().unwrap();
    let v6: SocketAddr = "[::1]:6881".parse().unwrap();
    let set_v4 = compute_allowed_fast_set(&info_hash, v4, 1000, 10);
    let set_v6 = compute_allowed_fast_set(&info_hash, v6, 1000, 10);
    assert_eq!(set_v4.len(), 10);
    assert_eq!(set_v6.len(), 10);
    // Should be different sets for different IPs
    assert_ne!(set_v4, set_v6);
}
