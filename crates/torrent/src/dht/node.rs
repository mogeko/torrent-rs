//! Session-level DHT node — shared across all torrents.
//!
//! A single [`DhtNode`] maintains one routing table, one UDP socket,
//! and one node ID. All torrents in the session share this node for
//! peer discovery (`get_peers`) and self-announcement (`announce_peer`).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rand::RngExt;
use tokio::sync::Mutex;
use torrent_core::dht::{Node, RoutingTable};

use crate::error::Error;

use super::{find_node, get_peers, krpc, rpc::DhtRpc};

/// Interval between periodic bootstrap refreshes.
const BOOTSTRAP_INTERVAL: Duration = Duration::from_secs(300);

/// Number of closest nodes to query in each round of `get_peers`.
const ALPHA: usize = 8;

/// Concurrency for parallel DHT queries during lookup.
const LOOKUP_CONCURRENCY: usize = 3;

/// Shared DHT node — one per session.
pub(crate) struct DhtNode {
    /// Our stable node ID (20-byte SHA-1).
    pub node_id: [u8; 20],
    /// The Kademlia routing table.
    routing_table: Arc<Mutex<RoutingTable>>,
    /// Async UDP RPC client.
    rpc: Arc<DhtRpc>,
    /// Well-known bootstrap addresses (resolved at construction time).
    bootstrap_nodes: Vec<SocketAddr>,
}

impl DhtNode {
    /// Initialize a new DHT node.
    ///
    /// Binds a UDP socket, generates a random node ID, resolves
    /// bootstrap hostnames, and spawns a periodic bootstrap task.
    pub async fn new(bind_addr: SocketAddr, bootstrap: &[(&str, u16)]) -> Result<Arc<Self>, Error> {
        let rpc = DhtRpc::new(bind_addr).await?;
        let node_id = torrent_core::dht::generate_node_id();
        let routing_table = Arc::new(Mutex::new(RoutingTable::with_id(node_id)));

        // Resolve bootstrap hostnames once
        let mut bootstrap_nodes = Vec::new();
        for (host, port) in bootstrap {
            if let Ok(mut addrs) = tokio::net::lookup_host((*host, *port)).await {
                if let Some(addr) = addrs.next() {
                    bootstrap_nodes.push(addr);
                }
            }
        }

        let node = Arc::new(DhtNode {
            node_id,
            routing_table,
            rpc,
            bootstrap_nodes,
        });

        // Perform initial bootstrap then spawn periodic refresher
        node.clone().bootstrap().await;
        node.clone().spawn_bootstrap_loop();

        Ok(node)
    }

    /// Bootstrap — query known bootstrap nodes to populate the routing table.
    ///
    /// Sends `find_node` queries with a random target to each bootstrap
    /// node. Any returned nodes are inserted into the routing table.
    async fn bootstrap(&self) {
        for &addr in &self.bootstrap_nodes {
            let tid: krpc::TransactionId = rand::rng().random();
            let target = torrent_core::dht::generate_node_id();

            if let Ok(nodes) = find_node(&self.rpc, addr, tid, &self.node_id, &target).await {
                let mut rt = self.routing_table.lock().await;
                for node in nodes {
                    rt.insert(node);
                }
            }
        }
    }

    /// Find peers for an info_hash via the DHT.
    ///
    /// Queries the K closest known nodes for peers sharing the given
    /// infohash. Currently performs a single round of parallel queries
    /// (iterative recursive lookup is deferred to Phase 4).
    pub async fn get_peers(&self, info_hash: &[u8; 20]) -> Vec<SocketAddr> {
        let closest = {
            let rt = self.routing_table.lock().await;
            rt.find_closest(info_hash, ALPHA)
        };

        if closest.is_empty() {
            return vec![];
        }

        let mut peers: HashSet<SocketAddr> = HashSet::new();
        let mut nodes_to_insert: Vec<Node> = Vec::new();
        let mut handles = Vec::new();

        for node in closest.into_iter().take(LOOKUP_CONCURRENCY) {
            let rpc = self.rpc.clone();
            let node_id = self.node_id;
            let target = *info_hash;
            let tid: krpc::TransactionId = rand::rng().random();

            handles.push(tokio::spawn(async move {
                get_peers(&rpc, node.addr, tid, &node_id, &target).await
            }));
        }

        for handle in handles {
            if let Ok(Ok(result)) = handle.await {
                match result {
                    krpc::GetPeersResult::Values {
                        peers: returned_peers,
                        ..
                    } => {
                        peers.extend(returned_peers);
                    }
                    krpc::GetPeersResult::Nodes(nodes) => {
                        nodes_to_insert.extend(nodes);
                    }
                }
            }
        }

        // Feed any returned nodes back into the routing table
        if !nodes_to_insert.is_empty() {
            let mut rt = self.routing_table.lock().await;
            for node in nodes_to_insert {
                rt.insert(node);
            }
        }

        peers.into_iter().collect()
    }

    /// Announce that we are a peer for an info_hash.
    ///
    /// Sends `announce_peer` to the closest known nodes. Token
    /// validation is deferred to Phase 4 (uses an empty token).
    #[expect(dead_code)]
    pub async fn announce_peer(&self, info_hash: &[u8; 20], port: u16) -> Result<(), Error> {
        let closest = {
            let rt = self.routing_table.lock().await;
            rt.find_closest(info_hash, ALPHA)
        };

        for node in &closest {
            let tid: krpc::TransactionId = rand::rng().random();
            let data = krpc::build_announce_peer(tid, &self.node_id, info_hash, port, b"");
            let _ = self.rpc.query(node.addr, tid, &data).await;
        }

        Ok(())
    }

    /// Spawn a periodic bootstrap task that re-populates the routing table.
    fn spawn_bootstrap_loop(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(BOOTSTRAP_INTERVAL).await;
                let node = self.clone();
                node.bootstrap().await;
            }
        });
    }
}
