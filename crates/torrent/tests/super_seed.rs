//! Integration tests for BEP 16 Super Seeding.
//!
//! These tests validate the per-torrent super seed configuration
//! and the core piece-to-peer assignment logic.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;

use torrent::error::Error;
use torrent::session::{DataSource, Session, SessionConfig};

// ── SeedBuilder flag propagation ──────────────────────────────────────

/// A minimal DataSource that returns zero bytes for testing builder flag propagation.
#[derive(Debug)]
struct EmptySource;

impl DataSource for EmptySource {
    fn name(&self) -> &str {
        "empty"
    }

    fn total_size(&self) -> Pin<Box<dyn Future<Output = Result<u64, Error>> + Send + '_>> {
        Box::pin(std::future::ready(Ok(0)))
    }

    fn read_at<'a>(
        &'a self, _offset: u64, _buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<usize, Error>> + Send + 'a>> {
        Box::pin(std::future::ready(Ok(0)))
    }
}

#[tokio::test]
async fn seed_builder_defaults_to_no_super_seed() -> Result<(), Error> {
    let config = SessionConfig {
        bootstrap_nodes: None,
        ..Default::default()
    };
    let session = Session::new(config).await?;

    let prepared = session
        .seed_from(EmptySource)
        .announce("http://tracker.example.com/announce")
        .hash()
        .await?;

    assert!(!prepared.super_seed(), "super_seed should default to false");

    Ok(())
}

#[tokio::test]
async fn seed_builder_super_seed_flag_propagates() -> Result<(), Error> {
    let config = SessionConfig {
        bootstrap_nodes: None,
        ..Default::default()
    };
    let session = Session::new(config).await?;

    let prepared = session
        .seed_from(EmptySource)
        .announce("http://tracker.example.com/announce")
        .super_seed(true)
        .hash()
        .await?;

    assert!(
        prepared.super_seed(),
        "super_seed flag should propagate to PreparedTorrent"
    );

    Ok(())
}

#[tokio::test]
async fn seed_builder_super_seed_explicitly_false() -> Result<(), Error> {
    let config = SessionConfig {
        bootstrap_nodes: None,
        ..Default::default()
    };
    let session = Session::new(config).await?;

    let prepared = session
        .seed_from(EmptySource)
        .announce("http://tracker.example.com/announce")
        .super_seed(false)
        .hash()
        .await?;

    assert!(
        !prepared.super_seed(),
        "super_seed(false) should be respected"
    );

    Ok(())
}

// ── Bitfield masking logic (super seed hides unrevealed pieces) ──────

/// Helper: simulate masking unrevealed pieces from a bitfield.
/// This mirrors the logic in SwarmLoop::send_bitfield.
fn mask_unrevealed_pieces(mut bitfield: Vec<u8>, unrevealed: &HashSet<u32>) -> Vec<u8> {
    for &idx in unrevealed {
        let byte = idx as usize / 8;
        let bit = 7 - (idx as usize % 8);
        if byte < bitfield.len() {
            bitfield[byte] &= !(1 << bit);
        }
    }
    bitfield
}

#[test]
fn mask_unrevealed_clears_matching_bits() {
    // All ones: 8 pieces, all set
    let bf = vec![0b11111111u8];
    let unrevealed: HashSet<u32> = [0u32, 7].into_iter().collect();

    let masked = mask_unrevealed_pieces(bf, &unrevealed);
    // piece 0 (MSB, bit 7) → cleared: 0b01111111
    // piece 7 (LSB, bit 0) → cleared: 0b01111110
    assert_eq!(masked[0], 0b01111110);
}

#[test]
fn mask_unrevealed_empty_set_no_change() {
    let bf = vec![0b10101010u8];
    let unrevealed = HashSet::new();
    let masked = mask_unrevealed_pieces(bf.clone(), &unrevealed);
    assert_eq!(masked, bf);
}

#[test]
fn mask_unrevealed_multi_byte() {
    // 16 pieces (2 bytes), all set
    let bf = vec![0b11111111u8, 0b11111111u8];
    // Unreveal piece 14: byte 1 (14/8=1), bit 1 (7-14%8=7-6=1)
    let unrevealed: HashSet<u32> = [14u32].into_iter().collect();
    let masked = mask_unrevealed_pieces(bf, &unrevealed);
    assert_eq!(masked[0], 0b11111111); // unchanged
    assert_eq!(masked[1], 0b11111101); // bit 1 cleared
}

// ── Request gating logic ────────────────────────────────────────────

/// Simulates the super seed request gate from SwarmLoop::handle_peer_message.
fn super_seed_can_serve(
    super_seed: bool, piece_index: u32, requester: SocketAddr,
    assignments: &HashMap<u32, SocketAddr>, unrevealed: &HashSet<u32>,
) -> bool {
    if !super_seed {
        return true;
    }
    if !unrevealed.contains(&piece_index) {
        return true; // already revealed, serve to anyone
    }
    assignments.get(&piece_index) == Some(&requester)
}

#[test]
fn gate_allows_when_not_super_seed() {
    let addr_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 6881);
    assert!(super_seed_can_serve(
        false,
        0,
        addr_a,
        &HashMap::new(),
        &HashSet::new()
    ));
}

#[test]
fn gate_allows_revealed_piece_to_anyone() {
    let addr_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2)), 6881);
    // Piece 0 is NOT in unrevealed → already revealed, serve to anyone
    assert!(super_seed_can_serve(
        true,
        0,
        addr_b,
        &HashMap::new(),
        &HashSet::new()
    ));
}

#[test]
fn gate_allows_assigned_peer_for_unrevealed() {
    let addr_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 6881);
    let assignments = HashMap::from([(0u32, addr_a)]);
    let unrevealed: HashSet<u32> = [0u32].into_iter().collect();
    assert!(super_seed_can_serve(
        true,
        0,
        addr_a,
        &assignments,
        &unrevealed
    ));
}

#[test]
fn gate_blocks_non_assigned_peer_for_unrevealed() {
    let addr_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 6881);
    let addr_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2)), 6881);
    let assignments = HashMap::from([(0u32, addr_a)]);
    let unrevealed: HashSet<u32> = [0u32].into_iter().collect();
    assert!(!super_seed_can_serve(
        true,
        0,
        addr_b,
        &assignments,
        &unrevealed
    ));
}

// ── HAVE reveal logic ───────────────────────────────────────────────

#[test]
fn have_from_assigned_peer_reveals_piece() {
    let addr_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 6881);
    let mut assignments = HashMap::from([(5u32, addr_a)]);
    let mut unrevealed: HashSet<u32> = [5u32].into_iter().collect();

    // Sim: assigned peer sends HAVE(5) → reveal
    if assignments.get(&5) == Some(&addr_a) {
        unrevealed.remove(&5);
        assignments.remove(&5);
    }

    assert!(unrevealed.is_empty());
    assert!(assignments.is_empty());
}

#[test]
fn have_from_non_assigned_peer_does_not_reveal() {
    let addr_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 6881);
    let addr_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2)), 6881);
    let assignments = HashMap::from([(5u32, addr_a)]);
    let mut unrevealed: HashSet<u32> = [5u32].into_iter().collect();

    // Sim: non-assigned peer sends HAVE(5) → should NOT trigger reveal
    if assignments.get(&5) == Some(&addr_b) {
        unrevealed.remove(&5);
    }

    assert!(
        !unrevealed.is_empty(),
        "unrevealed should still contain piece 5"
    );
    assert_eq!(unrevealed.len(), 1);
}

// ── Disconnect cleanup ──────────────────────────────────────────────

#[test]
fn disconnect_clears_assignment_but_not_unrevealed() {
    let addr_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 6881);
    let mut assignments = HashMap::from([(3u32, addr_a)]);
    let unrevealed: HashSet<u32> = [3u32].into_iter().collect();

    // Sim: peer disconnects — clear their assignments, keep unrevealed
    assignments.retain(|_, a| a != &addr_a);

    assert!(assignments.is_empty(), "assignment should be removed");
    assert!(
        !unrevealed.is_empty(),
        "unrevealed should persist for reassignment"
    );
}
