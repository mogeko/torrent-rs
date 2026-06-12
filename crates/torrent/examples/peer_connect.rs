//! Low-level peer wire protocol: handshake, message encode/decode.
//!
//! This example shows how to construct and parse the 68-byte handshake
//! and all 11 wire protocol messages. The actual TCP connection is
//! commented out — use a real peer address to try it live.
//!
//! Run with: `cargo run -p torrent --example peer_connect`

use torrent::peer::{Handshake, PeerId, PeerMessage, PeerState, decode, encode};

fn main() {
    let info_hash = [0x42u8; 20];
    let our_id = PeerId::random();
    println!("Our peer ID: {}", our_id);

    // --- Handshake ---
    let hs = Handshake::new(info_hash, our_id.0);
    let hs_bytes = hs.to_bytes();
    println!("\nHandshake: {} bytes", hs_bytes.len());

    let parsed = Handshake::from_bytes(&hs_bytes).expect("invalid handshake");
    assert_eq!(hs, parsed);
    println!("Round-trip: OK");

    // --- Message types ---
    println!("\n=== All 11 Message Types ===");
    let messages = [
        ("KeepAlive", PeerMessage::KeepAlive),
        ("Choke", PeerMessage::Choke),
        ("Unchoke", PeerMessage::Unchoke),
        ("Interested", PeerMessage::Interested),
        ("NotInterested", PeerMessage::NotInterested),
        ("Have(42)", PeerMessage::Have(42)),
        ("Bitfield([0xFF])", PeerMessage::Bitfield(vec![0xFF])),
        (
            "Request",
            PeerMessage::Request {
                index: 0,
                begin: 0,
                length: 16384,
            },
        ),
        (
            "Piece",
            PeerMessage::Piece {
                index: 0,
                begin: 0,
                data: vec![0xAA; 16],
            },
        ),
        (
            "Cancel",
            PeerMessage::Cancel {
                index: 1,
                begin: 1024,
                length: 8192,
            },
        ),
        ("Port(6881)", PeerMessage::Port(6881)),
    ];

    for (name, msg) in &messages {
        let wire = encode(msg);
        let decoded = decode(&wire).expect("decode failed");
        assert_eq!(*msg, decoded, "roundtrip failed for {}", name);
        println!("  {:16} → {} bytes  roundtrip OK", name, wire.len());
    }

    // --- Connection state machine ---
    println!("\n=== PeerState ===");
    let states = [
        PeerState::Handshake,
        PeerState::Init,
        PeerState::Unchoked,
        PeerState::Choked,
        PeerState::Closed,
    ];
    for s in &states {
        println!("  {:?}", s);
    }

    // --- Async PeerConnection ---
    // For an end-to-end local peer connection demo, see the `peer_pair` example:
    //   cargo run -p torrent --example peer_pair
}
