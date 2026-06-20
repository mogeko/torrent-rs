//! Kademlia Distributed Hash Table (BEP 5) — sync types.
//!
//! This module provides the sync primitives for DHT communication:
//! - [`RoutingTable`] — K-bucket management (insert, lookup, find_closest)
//! - [`krpc::KrpcMessage`] — KRPC message encode/decode
//!
//! Async RPC and query helpers live in the `torrent` crate under `torrent::dht`.

pub mod krpc;

mod kbucket;

use std::net::SocketAddr;

use rand::RngExt;
use sha1::{Digest, Sha1};

use self::kbucket::KBucket;

/// Number of buckets (160-bit address space).
const NUM_BUCKETS: usize = 160;

/// Represents a node in the DHT (BEP 5).
///
/// Each node is identified by a 20-byte Node ID (typically a SHA-1 hash)
/// and reachable at a socket address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    /// 20-byte Node ID.
    pub id: [u8; 20],
    /// Socket address (IP + port).
    pub addr: SocketAddr,
}

/// A DHT bootstrap node address (BEP 5).
///
/// Represents a well-known DHT node used to join the DHT network.
/// The hostname is resolved to a [`SocketAddr`] at connection time.
///
/// # Examples
///
/// ```
/// use torrent_core::dht::BootstrapNode;
///
/// let node = BootstrapNode::from(("router.bittorrent.com", 6881));
/// assert_eq!(node.host, "router.bittorrent.com");
/// assert_eq!(node.port, 6881);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct BootstrapNode {
    /// Hostname of the bootstrap node.
    pub host: String,
    /// UDP port of the bootstrap node.
    pub port: u16,
}

impl From<(String, u16)> for BootstrapNode {
    fn from((host, port): (String, u16)) -> Self {
        BootstrapNode { host, port }
    }
}

impl From<(&str, u16)> for BootstrapNode {
    fn from((host, port): (&str, u16)) -> Self {
        BootstrapNode {
            host: host.to_owned(),
            port,
        }
    }
}

/// Kademlia routing table (BEP 5) — single address family.
///
/// Maintains 160 K-buckets, each holding up to K=8 nodes
/// ordered by XOR distance from our node ID. Nodes are inserted
/// into the bucket determined by the first differing bit between
/// their ID and ours.
///
/// Use [`find_closest`](RoutingTable::find_closest) to discover
/// nodes near a target ID (used in recursive DHT lookups).
pub struct RoutingTable {
    /// Our own node ID.
    pub node_id: [u8; 20],
    /// K-buckets: 160 buckets, each with up to K nodes.
    buckets: Vec<KBucket>,
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

impl RoutingTable {
    /// Create a new routing table with a random node ID (SHA-1 of random data).
    pub fn new() -> Self {
        Self::with_id(generate_node_id())
    }

    /// Create a routing table with a specific node ID.
    pub fn with_id(node_id: [u8; 20]) -> Self {
        Self {
            node_id,
            buckets: (0..NUM_BUCKETS).map(|_| KBucket::new()).collect(),
        }
    }

    /// Insert or update a node in the routing table.
    ///
    /// Delegates to the appropriate K-bucket which handles LRU ordering
    /// and eviction. Returns `true` if the node was newly added.
    pub fn insert(&mut self, node: Node) -> bool {
        insert_into_buckets(&mut self.buckets, &self.node_id, node)
    }

    /// Find the K closest nodes to a target ID.
    pub fn find_closest(&self, target: &[u8; 20], count: usize) -> Vec<Node> {
        find_closest_in_buckets(&self.buckets, target, count)
    }

    /// Number of known nodes in the routing table.
    pub fn num_nodes(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }
}

/// Dual-stack routing table (BEP 32).
///
/// Maintains two independent sets of 160 K-buckets — one for IPv4
/// and one for IPv6. BEP 32 defines IPv4 and IPv6 DHTs as separate
/// networks; both share a single node ID.
///
/// Currently unused — reserved for future cross-family bootstrap support.
pub struct DualRoutingTable {
    /// Shared node ID (BEP 32 §3.2 recommends same ID for both DHTs).
    pub node_id: [u8; 20],
    /// IPv4 K-buckets (160 buckets).
    buckets_v4: Vec<KBucket>,
    /// IPv6 K-buckets (160 buckets).
    buckets_v6: Vec<KBucket>,
}

impl DualRoutingTable {
    /// Create a new dual-stack routing table with a random node ID.
    pub fn new() -> Self {
        Self::with_id(generate_node_id())
    }

    /// Create a new dual-stack routing table with a specific node ID.
    pub fn with_id(node_id: [u8; 20]) -> Self {
        Self {
            node_id,
            buckets_v4: (0..NUM_BUCKETS).map(|_| KBucket::new()).collect(),
            buckets_v6: (0..NUM_BUCKETS).map(|_| KBucket::new()).collect(),
        }
    }

    /// Insert or update a node, dispatching to the correct address-family buckets.
    pub fn insert(&mut self, node: Node) -> bool {
        if node.addr.is_ipv4() {
            insert_into_buckets(&mut self.buckets_v4, &self.node_id, node)
        } else {
            insert_into_buckets(&mut self.buckets_v6, &self.node_id, node)
        }
    }

    /// Find the K closest nodes to a target ID, merging both families.
    pub fn find_closest(&self, target: &[u8; 20], count: usize) -> Vec<Node> {
        let v4 = self.find_closest_v4(target, count);
        let v6 = self.find_closest_v6(target, count);
        let mut all = v4;
        all.extend(v6);
        all.sort_by_key(|n| xor_distance(&n.id, target));
        all.truncate(count);
        all
    }

    /// Find the K closest IPv4 nodes to a target ID.
    pub fn find_closest_v4(&self, target: &[u8; 20], count: usize) -> Vec<Node> {
        find_closest_in_buckets(&self.buckets_v4, target, count)
    }

    /// Find the K closest IPv6 nodes to a target ID.
    pub fn find_closest_v6(&self, target: &[u8; 20], count: usize) -> Vec<Node> {
        find_closest_in_buckets(&self.buckets_v6, target, count)
    }

    /// Total number of known nodes across both families.
    pub fn num_nodes(&self) -> usize {
        self.num_nodes_v4() + self.num_nodes_v6()
    }

    /// Number of known IPv4 nodes.
    pub fn num_nodes_v4(&self) -> usize {
        self.buckets_v4.iter().map(|b| b.len()).sum()
    }

    /// Number of known IPv6 nodes.
    pub fn num_nodes_v6(&self) -> usize {
        self.buckets_v6.iter().map(|b| b.len()).sum()
    }
}

impl Default for DualRoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for DualRoutingTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DualRoutingTable")
            .field("num_nodes_v4", &self.num_nodes_v4())
            .field("num_nodes_v6", &self.num_nodes_v6())
            .finish()
    }
}

// ── Shared bucket helpers ─────────────────────────────────────────

fn insert_into_buckets(buckets: &mut [KBucket], our_id: &[u8; 20], node: Node) -> bool {
    tracing::debug!("DHT insert: {}", node.addr);
    let idx = bucket_index(our_id, &node.id);
    buckets[idx].insert(node)
}

fn find_closest_in_buckets(buckets: &[KBucket], target: &[u8; 20], count: usize) -> Vec<Node> {
    let mut all: Vec<&Node> = buckets.iter().flat_map(|b| b.iter()).collect();
    all.sort_by_key(|n| xor_distance(&n.id, target));
    all.into_iter().take(count).cloned().collect()
}

/// Compute the XOR distance between two 160-bit IDs as a big integer.
fn xor_distance(a: &[u8; 20], b: &[u8; 20]) -> [u8; 20] {
    let mut dist = [0u8; 20];
    for i in 0..20 {
        dist[i] = a[i] ^ b[i];
    }
    dist
}

/// Determine which bucket a node belongs in based on its ID.
///
/// The bucket index is the position of the most significant bit
/// that differs from our node ID (0-indexed from high bit).
fn bucket_index(our_id: &[u8; 20], node_id: &[u8; 20]) -> usize {
    for i in 0..20 {
        let diff = our_id[i] ^ node_id[i];
        if diff != 0 {
            let leading_zeros = diff.leading_zeros() as usize;
            return i * 8 + leading_zeros;
        }
    }
    0 // same ID — should not normally happen
}

/// Generate a random 20-byte node ID using SHA-1.
pub fn generate_node_id() -> [u8; 20] {
    let seed: u64 = rand::rng().random();
    let mut hasher = Sha1::new();
    hasher.update(seed.to_be_bytes());
    hasher.update(b"torrent-rs-dht-node");
    let result = hasher.finalize();
    let mut id = [0u8; 20];
    id.copy_from_slice(&result);
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xor_distance_same() {
        let id = [0x42u8; 20];
        assert_eq!(xor_distance(&id, &id), [0u8; 20]);
    }

    #[test]
    fn xor_distance_different() {
        let a = [0x00u8; 20];
        let mut b = [0x00u8; 20];
        b[0] = 0x01;
        let dist = xor_distance(&a, &b);
        assert_eq!(dist[0], 0x01);
    }

    #[test]
    fn bucket_index_first_byte_diff() {
        let our = [0x00u8; 20];
        let node = [0x80u8; 20]; // bit 7 differs (MSB)
        assert_eq!(bucket_index(&our, &node), 0);
    }

    #[test]
    fn bucket_index_second_byte() {
        let our = [0x00u8; 20];
        let mut node = [0x00u8; 20];
        node[1] = 0x01;
        // First byte same, second byte: 0x01 has 7 leading zeros
        // Bucket = 1*8 + 7 = 15
        let idx = bucket_index(&our, &node);
        assert_eq!(idx, 15);
    }

    #[test]
    fn routing_table_insert() {
        let mut rt = RoutingTable::new();
        let node = Node {
            id: [0x01u8; 20],
            addr: "127.0.0.1:6881".parse().unwrap(),
        };
        assert!(rt.insert(node));
        assert_eq!(rt.num_nodes(), 1);
    }

    #[test]
    fn routing_table_find_closest() {
        let mut rt = RoutingTable::with_id([0xFFu8; 20]);

        for i in 0..16 {
            let mut id = [0u8; 20];
            id[0] = i;
            rt.insert(Node {
                id,
                addr: "127.0.0.1:6881".parse().unwrap(),
            });
        }

        let target = [0x0Au8; 20];
        let closest = rt.find_closest(&target, 4);
        assert_eq!(closest.len(), 4);
    }

    #[test]
    fn routing_table_max_per_bucket() {
        let mut rt = RoutingTable::with_id([0x00u8; 20]);
        // All these nodes go to same bucket (differing in first byte)
        for i in 0..12 {
            let mut id = [0x00u8; 20];
            id[0] = 0x80 + i; // high bit differs
            rt.insert(Node {
                id,
                addr: "127.0.0.1:6881".parse().unwrap(),
            });
        }
        // K=8, so only the last 8 survive
        assert_eq!(rt.num_nodes(), 8);
    }

    #[test]
    fn node_id_generation() {
        let id1 = generate_node_id();
        let id2 = generate_node_id();
        assert_eq!(id1.len(), 20);
        assert_eq!(id2.len(), 20);
        // Should be different with high probability
        assert_ne!(id1, id2);
    }

    // ── A.3: find_closest ordering correctness ──────────────────

    #[test]
    fn find_closest_returns_correct_order() {
        let mut rt = RoutingTable::with_id([0x00u8; 20]);

        // Insert nodes with increasing distance
        for i in 1u8..=8 {
            let mut id = [0u8; 20];
            id[0] = i;
            rt.insert(Node {
                id,
                addr: "127.0.0.1:6881".parse().unwrap(),
            });
        }

        // Target is 0 — so node with id[0]=1 should be closest
        let target = [0x00u8; 20];
        let closest = rt.find_closest(&target, 4);
        assert_eq!(closest.len(), 4);

        // Verify order: id[0]=1 < id[0]=2 < id[0]=3 < id[0]=4
        for (i, node) in closest.iter().enumerate() {
            assert_eq!(node.id[0], (i + 1) as u8);
        }
    }

    #[test]
    fn find_closest_count_exceeds_total() {
        let mut rt = RoutingTable::with_id([0u8; 20]);
        rt.insert(Node {
            id: [0x01u8; 20],
            addr: "127.0.0.1:6881".parse().unwrap(),
        });
        rt.insert(Node {
            id: [0x02u8; 20],
            addr: "127.0.0.1:6882".parse().unwrap(),
        });

        // Request more than we have
        let closest = rt.find_closest(&[0x00u8; 20], 10);
        assert_eq!(closest.len(), 2);
    }

    // ── A.4: bucket_index edge cases ────────────────────────────

    #[test]
    fn bucket_index_last_bit_diff() {
        let our = [0x00u8; 20];
        let mut node = [0x00u8; 20];
        node[19] = 0x01; // last byte, bit 0 → bucket = 19*8 + 7 = 159
        assert_eq!(bucket_index(&our, &node), 159);
    }

    #[test]
    fn bucket_index_same_id() {
        let id = [0x42u8; 20];
        assert_eq!(bucket_index(&id, &id), 0);
    }

    // ── DualRoutingTable tests (BEP 32) ────────────────────────

    #[test]
    fn dual_table_new_is_empty() {
        let table = DualRoutingTable::new();
        assert_eq!(table.num_nodes(), 0);
        assert_eq!(table.num_nodes_v4(), 0);
        assert_eq!(table.num_nodes_v6(), 0);
    }

    #[test]
    fn dual_table_insert_dispatches_by_family() {
        let mut table = DualRoutingTable::with_id([0x42u8; 20]);
        let v4 = Node {
            id: [0x01u8; 20],
            addr: "10.0.0.1:6881".parse().unwrap(),
        };
        let v6 = Node {
            id: [0x02u8; 20],
            addr: "[::1]:6881".parse().unwrap(),
        };
        assert!(table.insert(v4));
        assert!(table.insert(v6));
        assert_eq!(table.num_nodes_v4(), 1);
        assert_eq!(table.num_nodes_v6(), 1);
        assert_eq!(table.num_nodes(), 2);
    }

    #[test]
    fn dual_table_find_closest_merges() {
        let mut table = DualRoutingTable::with_id([0x00u8; 20]);
        let v4 = Node {
            id: [0x01u8; 20],
            addr: "10.0.0.1:6881".parse().unwrap(),
        };
        let v6 = Node {
            id: [0x02u8; 20],
            addr: "[::1]:6881".parse().unwrap(),
        };
        table.insert(v4.clone());
        table.insert(v6.clone());
        let closest = table.find_closest(&[0x00u8; 20], 2);
        assert_eq!(closest.len(), 2);
        assert_eq!(closest[0], v4);
        assert_eq!(closest[1], v6);
    }

    #[test]
    fn dual_table_find_closest_v4_only_ipv4() {
        let mut table = DualRoutingTable::with_id([0x00u8; 20]);
        table.insert(Node {
            id: [0x01u8; 20],
            addr: "10.0.0.1:6881".parse().unwrap(),
        });
        table.insert(Node {
            id: [0x02u8; 20],
            addr: "[::1]:6881".parse().unwrap(),
        });
        let closest = table.find_closest_v4(&[0x00u8; 20], 2);
        assert_eq!(closest.len(), 1);
        assert!(closest[0].addr.is_ipv4());
    }

    #[test]
    fn dual_table_find_closest_v6_only_ipv6() {
        let mut table = DualRoutingTable::with_id([0x00u8; 20]);
        table.insert(Node {
            id: [0x01u8; 20],
            addr: "10.0.0.1:6881".parse().unwrap(),
        });
        table.insert(Node {
            id: [0x02u8; 20],
            addr: "[::1]:6881".parse().unwrap(),
        });
        let closest = table.find_closest_v6(&[0x00u8; 20], 2);
        assert_eq!(closest.len(), 1);
        assert!(closest[0].addr.is_ipv6());
    }

    #[test]
    fn dual_table_shares_node_id() {
        let id = [0xABu8; 20];
        let table = DualRoutingTable::with_id(id);
        assert_eq!(table.node_id, id);
    }
}

#[cfg(all(test, feature = "serde"))]
mod serde_tests {
    use super::*;

    #[test]
    fn bootstrap_node_roundtrip() {
        let node = BootstrapNode::from(("router.bittorrent.com", 6881));
        let json = serde_json::to_string(&node).unwrap();
        let back: BootstrapNode = serde_json::from_str(&json).unwrap();
        assert_eq!(back.host, "router.bittorrent.com");
        assert_eq!(back.port, 6881);
    }

    #[test]
    fn bootstrap_node_empty_host() {
        let node = BootstrapNode {
            host: String::new(),
            port: 0,
        };
        let json = serde_json::to_string(&node).unwrap();
        let back: BootstrapNode = serde_json::from_str(&json).unwrap();
        assert_eq!(back.host, "");
        assert_eq!(back.port, 0);
    }

    #[test]
    fn bootstrap_node_high_port() {
        let node = BootstrapNode {
            host: "example.com".into(),
            port: 65535,
        };
        let json = serde_json::to_string(&node).unwrap();
        let back: BootstrapNode = serde_json::from_str(&json).unwrap();
        assert_eq!(back.host, "example.com");
        assert_eq!(back.port, 65535);
    }
}
