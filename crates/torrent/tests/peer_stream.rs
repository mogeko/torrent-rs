//! Integration tests for async peer communication.
//!
//! Focuses on cross-module integration. Protocol-level roundtrip
//! tests live in the corresponding unit tests (crates/torrent-core/src/peer/).

use torrent::peer::{PeerId, PeerMessage, PeerState, decode, encode};

#[test]
fn re_exports_work() {
    // Verify all key types are accessible from torrent::peer
    let peer_id = PeerId::random();
    assert_eq!(peer_id.0.len(), 20);
    assert_eq!(&peer_id.0[..8], b"-TR1000-");
}

#[test]
fn peer_state_variants() {
    // Verify PeerState enum variants exist
    let states = vec![
        PeerState::Handshake,
        PeerState::Init,
        PeerState::Unchoked,
        PeerState::Choked,
        PeerState::Closed,
    ];
    assert_eq!(states.len(), 5);
}

#[test]
fn encode_decode_idempotent() {
    // encode(decode(encode(x))) == encode(x)
    let msg = PeerMessage::Request {
        index: 42,
        begin: 0,
        length: 65536,
    };
    let encoded = encode(&msg);
    let decoded = decode(&encoded).unwrap();
    let re_encoded = encode(&decoded);
    assert_eq!(encoded, re_encoded);
}
