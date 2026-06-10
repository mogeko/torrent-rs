//! Integration tests for async peer communication.

use torrent::peer::{Handshake, PeerId, PeerMessage, PeerState, decode, encode};

#[test]
fn re_exports_work() {
    // Verify all key types are accessible from torrent::peer
    let peer_id = PeerId::random();
    assert_eq!(peer_id.0.len(), 20);
    assert_eq!(&peer_id.0[..8], b"-TR1000-");
}

#[test]
fn handshake_roundtrip() {
    let peer_id = PeerId::random();
    let hs = Handshake::new([1u8; 20], peer_id.0);
    let bytes = hs.to_bytes();
    assert_eq!(bytes.len(), 68);

    let parsed = Handshake::from_bytes(&bytes).unwrap();
    assert_eq!(hs, parsed);
}

#[test]
fn message_roundtrip_all_types() {
    let messages = vec![
        PeerMessage::KeepAlive,
        PeerMessage::Choke,
        PeerMessage::Unchoke,
        PeerMessage::Interested,
        PeerMessage::NotInterested,
        PeerMessage::Have(7),
        PeerMessage::Bitfield(vec![0xFF]),
        PeerMessage::Request {
            index: 0,
            begin: 0,
            length: 16384,
        },
        PeerMessage::Piece {
            index: 0,
            begin: 0,
            data: vec![0xAA; 16384],
        },
        PeerMessage::Cancel {
            index: 1,
            begin: 1024,
            length: 8192,
        },
        PeerMessage::Port(6881),
    ];

    for msg in &messages {
        let encoded = encode(msg);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(*msg, decoded, "roundtrip failed for {:?}", msg);
    }
}

#[test]
fn message_encode_lengths() {
    assert_eq!(encode(&PeerMessage::KeepAlive).len(), 4);
    assert_eq!(encode(&PeerMessage::Choke).len(), 5);
    assert_eq!(encode(&PeerMessage::Have(0)).len(), 9);
    assert_eq!(encode(&PeerMessage::Port(6881)).len(), 7);
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
    // encode(decode(x)) == x
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
