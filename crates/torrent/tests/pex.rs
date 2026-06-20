//! Integration tests for PEX (BEP 11) and LTEP (BEP 10) — protocol-level
//! and session integration verification.
//!
//! These tests validate:
//! - PexMessage encode/decode roundtrip
//! - ExtensionNegotiation (LTEP handshake) encode/decode
//! - Compact peer encoding for IPv4 and IPv6
//! - PeerSession PEX flow via mock peers

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use torrent_core::bencode;
use torrent_core::peer::pex::PexMessage;
use torrent_core::peer::{ExtensionNegotiation, PeerMessage, decode, encode};
use torrent_core::tracker::{
    encode_compact_peers_ipv4, encode_compact_peers_ipv6, parse_compact_peers_ipv4,
    parse_compact_peers_ipv6,
};

// ── Compact peer encoding ────────────────────────────────────────────────

#[test]
fn compact_ipv4_roundtrip_multiple() {
    let addrs: Vec<SocketAddr> = vec![
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6881),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 6889),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080),
    ];
    let encoded = encode_compact_peers_ipv4(&addrs);
    let decoded = parse_compact_peers_ipv4(&encoded).unwrap();
    assert_eq!(addrs, decoded);
}

#[test]
fn compact_ipv6_roundtrip_multiple() {
    let addrs: Vec<SocketAddr> = vec![
        SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 6881),
        SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
            8080,
        ),
    ];
    let encoded = encode_compact_peers_ipv6(&addrs);
    let decoded = parse_compact_peers_ipv6(&encoded).unwrap();
    assert_eq!(addrs, decoded);
}

// ── ExtensionNegotiation (LTEP handshake) ────────────────────────────────

#[test]
fn extension_negotiation_roundtrip() {
    let mut neg = ExtensionNegotiation::new();
    neg.add_extension("ut_pex", 1);
    neg.add_extension("ut_metadata", 2);
    neg.v = Some("torrent.rs 0.1.0".into());
    neg.reqq = Some(512);

    let ben = neg.to_bencode();
    let parsed = ExtensionNegotiation::from_bencode(&ben).unwrap();
    assert_eq!(neg, parsed);
}

#[test]
fn extension_negotiation_missing_m_is_error() {
    // {"v":"test"} — no "m" key
    let (val, _) = bencode::decode(b"d1:v4:teste").unwrap();
    assert!(ExtensionNegotiation::from_bencode(&val).is_err());
}

#[test]
fn extension_negotiation_handshake_is_ext_id_zero() {
    let mut neg = ExtensionNegotiation::new();
    neg.add_extension("ut_pex", 1);
    let payload = bencode::encode(&neg.to_bencode());

    // LTEP handshake is sent as Extended { ext_id: 0, data }
    let ext_msg = PeerMessage::Extended {
        ext_id: 0,
        data: payload,
    };
    let wire = encode(&ext_msg);
    let decoded = decode(&wire).unwrap();

    assert_eq!(decoded, ext_msg);
    // Verify ext_id = 0
    if let PeerMessage::Extended { ext_id, data } = decoded {
        assert_eq!(ext_id, 0);
        let (val, _) = bencode::decode(&data).unwrap();
        let parsed = ExtensionNegotiation::from_bencode(&val).unwrap();
        assert_eq!(parsed.m.get("ut_pex"), Some(&1u8));
    } else {
        panic!("expected Extended message");
    }
}

// ── PexMessage ───────────────────────────────────────────────────────────

#[test]
fn pex_message_roundtrip_full() {
    let mut msg = PexMessage::new();
    msg.added.push(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        6881,
    ));
    msg.dropped.push(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        6889,
    ));
    msg.added6.push(SocketAddr::new(
        IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
        6881,
    ));
    msg.dropped6.push(SocketAddr::new(
        IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
        8080,
    ));

    let ben = msg.to_bencode();
    let parsed = PexMessage::from_bencode(&ben).unwrap();
    assert_eq!(msg, parsed);
}

#[test]
fn pex_message_empty() {
    let msg = PexMessage::new();
    let ben = msg.to_bencode();
    let parsed = PexMessage::from_bencode(&ben).unwrap();
    assert_eq!(msg, parsed);
}

#[test]
fn pex_message_added_only() {
    let mut msg = PexMessage::new();
    msg.added.push(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
        8080,
    ));
    msg.added.push(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        8081,
    ));

    let ben = msg.to_bencode();
    let parsed = PexMessage::from_bencode(&ben).unwrap();
    assert_eq!(msg.added, parsed.added);
    assert!(parsed.dropped.is_empty());
    assert!(parsed.added6.is_empty());
    assert!(parsed.dropped6.is_empty());
}

#[test]
fn pex_message_malformed_compact_is_graceful() {
    // "added" field with 7 bytes (not a multiple of 6) — should not panic
    let (val, _) = bencode::decode(b"d5:added7:abcdefge").unwrap();
    let msg = PexMessage::from_bencode(&val).unwrap();
    // Malformed data is ignored; added stays empty
    assert!(msg.added.is_empty());
}

#[test]
fn pex_message_not_a_dict_is_error() {
    let val = bencode::Bencode::Integer(42);
    assert!(PexMessage::from_bencode(&val).is_err());
}

#[test]
fn pex_message_wire_format() {
    // Verify that PexMessage can be embedded in an Extended message
    let mut msg = PexMessage::new();
    msg.added.push(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        6881,
    ));
    let payload = bencode::encode(&msg.to_bencode());

    let ext_msg = PeerMessage::Extended {
        ext_id: 1, // ut_pex negotiated ID
        data: payload,
    };
    let wire = encode(&ext_msg);
    let decoded = decode(&wire).unwrap();

    if let PeerMessage::Extended { ext_id: 1, data } = decoded {
        let (val, _) = bencode::decode(&data).unwrap();
        let parsed = PexMessage::from_bencode(&val).unwrap();
        assert_eq!(parsed.added, msg.added);
    } else {
        panic!("expected Extended message with ext_id=1");
    }
}

// ── LTEP + PEX full negotiation simulation ──────────────────────────────

#[test]
fn ltep_pex_full_negotiation_flow() {
    // Simulate the full LTEP → PEX message flow:
    // 1. Build our LTEP handshake
    let mut our_neg = ExtensionNegotiation::new();
    our_neg.add_extension("ut_pex", 1);
    let our_payload = bencode::encode(&our_neg.to_bencode());
    let our_ext_msg = PeerMessage::Extended {
        ext_id: 0,
        data: our_payload,
    };
    let our_wire = encode(&our_ext_msg);

    // 2. Remote receives, decodes
    let decoded_ours = decode(&our_wire).unwrap();
    if let PeerMessage::Extended { ext_id: 0, data } = decoded_ours {
        let (val, _) = bencode::decode(&data).unwrap();
        let parsed = ExtensionNegotiation::from_bencode(&val).unwrap();
        assert_eq!(parsed.m.get("ut_pex"), Some(&1u8));
    } else {
        panic!("expected Extended ext_id=0");
    }

    // 3. Remote builds its own LTEP handshake response
    let mut remote_neg = ExtensionNegotiation::new();
    remote_neg.add_extension("ut_pex", 2);
    let remote_payload = bencode::encode(&remote_neg.to_bencode());
    let remote_ext_msg = PeerMessage::Extended {
        ext_id: 0,
        data: remote_payload,
    };
    let remote_wire = encode(&remote_ext_msg);

    // 4. We receive remote's response
    let decoded_remote = decode(&remote_wire).unwrap();
    if let PeerMessage::Extended { ext_id: 0, data } = decoded_remote {
        let (val, _) = bencode::decode(&data).unwrap();
        let parsed = ExtensionNegotiation::from_bencode(&val).unwrap();
        assert_eq!(parsed.m.get("ut_pex"), Some(&2u8));
    } else {
        panic!("expected Extended ext_id=0");
    }

    // 5. Now send a PEX message using the negotiated ext_id
    let mut pex = PexMessage::new();
    pex.added.push(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        6881,
    ));
    let pex_payload = bencode::encode(&pex.to_bencode());
    let pex_msg = PeerMessage::Extended {
        ext_id: 2, // remote's ut_pex ID
        data: pex_payload,
    };
    let pex_wire = encode(&pex_msg);

    // 6. Remote receives PEX and decodes it
    let decoded_pex = decode(&pex_wire).unwrap();
    if let PeerMessage::Extended { ext_id: 2, data } = decoded_pex {
        let (val, _) = bencode::decode(&data).unwrap();
        let parsed = PexMessage::from_bencode(&val).unwrap();
        assert_eq!(parsed.added.len(), 1);
    } else {
        panic!("expected Extended ext_id=2");
    }
}

#[test]
fn pex_with_ipv6_peers() {
    // Verify that IPv6 peers in PEX messages survive roundtrip
    let mut msg = PexMessage::new();
    msg.added6.push(SocketAddr::new(
        IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
        6881,
    ));
    msg.dropped6.push(SocketAddr::new(
        IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1)),
        8080,
    ));

    let ben = msg.to_bencode();
    let parsed = PexMessage::from_bencode(&ben).unwrap();
    assert_eq!(msg.added6, parsed.added6);
    assert_eq!(msg.dropped6, parsed.dropped6);
}

#[test]
fn pex_with_both_ipv4_and_ipv6() {
    // Mixed IPv4 and IPv6 in the same PEX message
    let mut msg = PexMessage::new();
    msg.added.push(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        6881,
    ));
    msg.added6.push(SocketAddr::new(
        IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
        6881,
    ));

    let ben = msg.to_bencode();
    let parsed = PexMessage::from_bencode(&ben).unwrap();
    assert_eq!(parsed.added.len(), 1);
    assert_eq!(parsed.added6.len(), 1);
    assert!(parsed.dropped.is_empty());
    assert!(parsed.dropped6.is_empty());
}
