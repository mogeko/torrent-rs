//! K-bucket with LRU eviction — BEP 5.
//!
//! Each bucket holds up to `K = 8` nodes ordered by recency:
//! the **head** is the least recently seen (stalest) node,
//! the **tail** is the most recently seen (freshest) node.

use std::collections::VecDeque;

use super::Node;

/// Maximum nodes per bucket (BEP 5: K = 8).
pub(crate) const K: usize = 8;

/// A K-bucket with LRU eviction (BEP 5).
///
/// Maintains at most [`K`] nodes in least-recently-seen order:
/// - The **head** is the least recently seen (stalest) node.
/// - The **tail** is the most recently seen (freshest) node.
///
/// Every mutation (insert, promote) atomically preserves both the
/// size invariant and the LRU order.
#[derive(Debug, Clone)]
pub(crate) struct KBucket {
    nodes: VecDeque<Node>,
}

impl KBucket {
    /// Create an empty K-bucket.
    pub(crate) fn new() -> Self {
        KBucket {
            nodes: VecDeque::new(),
        }
    }

    /// Insert or refresh a node.
    ///
    /// - If the node is **new** and the bucket has room, it is appended
    ///   at the tail (freshest).
    /// - If the node is **new** and the bucket is full, the stalest
    ///   node (head) is evicted and the new node is appended.
    /// - If the node **already exists**, it is moved to the tail
    ///   (promoted as freshest) and its address is updated.
    ///
    /// Returns `true` if the node was newly inserted, `false` if it
    /// was merely refreshed (updated + promoted).
    pub(crate) fn insert(&mut self, node: Node) -> bool {
        // Promote existing node to tail
        if let Some(pos) = self.nodes.iter().position(|n| n.id == node.id) {
            self.nodes.remove(pos);
            self.nodes.push_back(node);
            return false;
        }

        // Evict stalest if full, then append new node
        if self.nodes.len() >= K {
            self.nodes.pop_front();
        }
        self.nodes.push_back(node);
        true
    }

    /// Iterate over stored nodes (head = stalest, tail = freshest).
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter()
    }

    /// Number of nodes currently in the bucket.
    pub(crate) fn len(&self) -> usize {
        self.nodes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn node(id_byte: u8) -> Node {
        let mut id = [0u8; 20];
        id[0] = id_byte;
        Node {
            id,
            addr: "127.0.0.1:6881".parse().unwrap(),
        }
    }

    #[test]
    fn new_is_empty() {
        let b = KBucket::new();
        assert_eq!(b.len(), 0);
        assert!(b.iter().next().is_none());
    }

    #[test]
    fn insert_new() {
        let mut b = KBucket::new();
        assert!(b.insert(node(1)));
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn insert_duplicate_promotes_to_tail() {
        let mut b = KBucket::new();
        let addr1: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.2:6881".parse().unwrap();

        let n1 = Node {
            id: [0x01; 20],
            addr: addr1,
        };
        assert!(b.insert(n1));
        assert_eq!(b.len(), 1);

        // Same ID, different addr → update + promote, returns false
        let n2 = Node {
            id: [0x01; 20],
            addr: addr2,
        };
        assert!(!b.insert(n2));
        assert_eq!(b.len(), 1);

        // Tail should reflect the updated address
        assert_eq!(b.iter().last().unwrap().addr, addr2);
    }

    #[test]
    fn insert_evicts_stalest_when_full() {
        let mut b = KBucket::new();
        for i in 0..K {
            assert!(b.insert(node(i as u8)));
        }
        assert_eq!(b.len(), K);

        // Insert one more → evicts the oldest (id[0] = 0)
        assert!(b.insert(node(0xFF)));
        assert_eq!(b.len(), K);

        let ids: Vec<u8> = b.iter().map(|n| n.id[0]).collect();
        assert!(!ids.contains(&0), "stalest node (id=0) should be evicted");
        assert!(ids.contains(&0xFF), "new node should replace evicted one");
    }

    #[test]
    fn iter_yields_recency_order() {
        let mut b = KBucket::new();
        b.insert(node(1));
        b.insert(node(2));
        b.insert(node(3));

        let ids: Vec<u8> = b.iter().map(|n| n.id[0]).collect();
        assert_eq!(ids, vec![1, 2, 3], "insert order = recency order");

        // Promote node(1) to tail
        b.insert(node(1));
        let ids: Vec<u8> = b.iter().map(|n| n.id[0]).collect();
        assert_eq!(ids, vec![2, 3, 1], "promoted node should be at tail");
    }

    #[test]
    fn len_after_mutations() {
        let mut b = KBucket::new();
        assert_eq!(b.len(), 0);

        b.insert(node(10));
        assert_eq!(b.len(), 1);

        b.insert(node(20));
        assert_eq!(b.len(), 2);

        // Fill to K, then evict one
        for i in 2..K {
            b.insert(node(i as u8));
        }
        assert_eq!(b.len(), K);

        b.insert(node(0xFF)); // evicts the oldest
        assert_eq!(b.len(), K);
    }
}
