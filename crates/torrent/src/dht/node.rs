//! Session-level DHT node — shared across all torrents.
//!
//! A single [`DhtNode`] maintains one routing table, one UDP socket,
//! and one node ID. All torrents in the session share this node for
//! peer discovery (`get_peers`) and self-announcement (`announce_peer`).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::RngExt;
use sha1::{Digest, Sha1};
use tokio::sync::Mutex;

use crate::error::Error;

use super::krpc::{self, KrpcMessage};
use super::rpc::{DhtRpc, QueryHandler};
use super::{Node, RoutingTable, find_node, generate_node_id, get_peers};

/// Interval between periodic bootstrap refreshes.
const BOOTSTRAP_INTERVAL: Duration = Duration::from_secs(300);

/// Number of closest nodes to query in each round of `get_peers`.
const ALPHA: usize = 8;

/// Concurrency for parallel DHT queries during lookup.
const LOOKUP_CONCURRENCY: usize = 3;

/// Shared DHT node — one per session per address family.
pub(crate) struct DhtNode {
    /// Our stable node ID (20-byte SHA-1).
    pub node_id: [u8; 20],
    /// The Kademlia routing table.
    routing_table: Arc<Mutex<RoutingTable>>,
    /// Async UDP RPC client.
    rpc: Arc<DhtRpc>,
    /// Well-known bootstrap addresses (resolved at construction time).
    bootstrap_nodes: Vec<SocketAddr>,
    /// Whether this node operates on IPv6 (affects `want` param and response keys).
    is_ipv6: bool,
    /// Secret for token generation (BEP 5 announce_peer validation).
    secret: [u8; 20],
}

impl DhtNode {
    /// Initialize a new DHT node with a specific node ID.
    ///
    /// Binds a UDP socket, resolves bootstrap hostnames, creates the
    /// routing table, installs the server-side query handler, and
    /// spawns a periodic bootstrap task.
    pub async fn new(
        node_id: [u8; 20], bind_addr: SocketAddr, bootstrap: &[(&str, u16)],
    ) -> Result<Arc<Self>, Error> {
        let rpc = DhtRpc::new(bind_addr).await?;
        let secret = generate_node_id(); // reuse SHA-1 generator for secret too
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
            is_ipv6: bind_addr.is_ipv6(),
            secret,
        });

        // Install server-side query handler
        let query_handler = create_query_handler(node.clone());
        node.rpc.set_query_handler(query_handler);

        // Kick off bootstrap in the background (non-blocking)
        node.clone().spawn_bootstrap();

        Ok(node)
    }

    /// Bootstrap — query known bootstrap nodes to populate the routing table.
    ///
    /// Sends `find_node` queries with a random target to each bootstrap
    /// node. Any returned nodes are inserted into the routing table.
    async fn bootstrap(&self) -> () {
        let want: Option<&[&str]> = if self.is_ipv6 {
            Some(&["n6"])
        } else {
            Some(&["n4"])
        };
        for &addr in &self.bootstrap_nodes {
            let tid: krpc::TransactionId = rand::rng().random();
            let target = generate_node_id();

            if let Ok(nodes) = find_node(&self.rpc, addr, tid, &self.node_id, &target, want).await {
                let mut rt = self.routing_table.lock().await;
                for node in nodes {
                    rt.insert(node);
                }
            }
        }
    }

    /// Find peers for an info_hash via iterative DHT lookup (BEP 5).
    ///
    /// Starts from the K closest known nodes and recursively queries
    /// closer nodes returned by each response. Stops when peers are
    /// found or a 15-second deadline expires.
    pub async fn get_peers(&self, info_hash: &[u8; 20]) -> Vec<SocketAddr> {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut queried: HashSet<[u8; 20]> = HashSet::new();
        let mut peers: HashSet<SocketAddr> = HashSet::new();

        // Seed: K closest nodes from the routing table
        let mut pending: Vec<Node> = {
            let rt = self.routing_table.lock().await;
            rt.find_closest(info_hash, ALPHA)
        };

        if pending.is_empty() {
            return vec![];
        }

        let want: Option<&[&str]> = if self.is_ipv6 {
            Some(&["n6"])
        } else {
            Some(&["n4"])
        };

        loop {
            // Take next batch of unqueried nodes
            let batch: Vec<Node> = pending
                .drain(..pending.len())
                .filter(|n| queried.insert(n.id))
                .take(LOOKUP_CONCURRENCY)
                .collect();

            if batch.is_empty() || Instant::now() >= deadline {
                break;
            }

            // Parallel queries
            let mut handles = Vec::with_capacity(batch.len());
            for node in &batch {
                let rpc = self.rpc.clone();
                let node_id = self.node_id;
                let target = *info_hash;
                let addr = node.addr;
                let tid: krpc::TransactionId = rand::rng().random();

                handles.push(tokio::spawn(async move {
                    get_peers(&rpc, addr, tid, &node_id, &target, want).await
                }));
            }

            // Drain handled responses — dynamic removal from Vec
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
                            // Feed into routing table, collect new unqueried nodes
                            let mut rt = self.routing_table.lock().await;
                            for node in nodes {
                                if !queried.contains(&node.id) {
                                    rt.insert(node.clone());
                                    pending.push(node);
                                }
                            }
                        }
                    }
                }
            }

            // Stop if we have enough peers or the deadline has passed
            if !peers.is_empty() || Instant::now() >= deadline {
                break;
            }

            // Refresh pending from routing table
            let rt = self.routing_table.lock().await;
            let fresh = rt.find_closest(info_hash, ALPHA);
            for node in fresh {
                if !queried.contains(&node.id) {
                    pending.push(node);
                }
            }
            if pending.is_empty() {
                break;
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

    /// Spawn a background task: bootstrap immediately, then
    /// periodically re-bootstrap to keep the routing table fresh.
    fn spawn_bootstrap(self: Arc<Self>) {
        tokio::spawn(async move {
            self.bootstrap().await;
            loop {
                tokio::time::sleep(BOOTSTRAP_INTERVAL).await;
                self.bootstrap().await;
            }
        });
    }
}

/// Build a query handler that responds to incoming DHT queries.
///
/// Uses the node's routing table and node ID to answer `ping`,
/// `find_node`, `get_peers`, and `announce_peer` queries.
fn create_query_handler(node: Arc<DhtNode>) -> QueryHandler {
    Arc::new(
        move |msg: &KrpcMessage, src: SocketAddr| -> Option<Vec<u8>> {
            let KrpcMessage::Query {
                transaction_id,
                method,
                args,
            } = msg
            else {
                return None;
            };
            let tid = *transaction_id;

            match method.as_str() {
                "ping" => Some(krpc::build_ping_response(tid, &node.node_id)),

                "find_node" => {
                    let target = krpc::dict_get_bytes(args, b"target")?;
                    let mut target_id = [0u8; 20];
                    let len = std::cmp::min(target.len(), 20);
                    target_id[..len].copy_from_slice(&target[..len]);

                    let rt = node.routing_table.blocking_lock();
                    let closest = rt.find_closest(&target_id, 8);
                    drop(rt);

                    Some(krpc::build_find_node_response(tid, &node.node_id, &closest))
                }

                "get_peers" => {
                    let info_hash_bytes = krpc::dict_get_bytes(args, b"info_hash")?;
                    let mut info_hash = [0u8; 20];
                    let len = std::cmp::min(info_hash_bytes.len(), 20);
                    info_hash[..len].copy_from_slice(&info_hash_bytes[..len]);
                    let token = generate_token(&node.secret, &src);

                    let rt = node.routing_table.blocking_lock();
                    let closest = rt.find_closest(&info_hash, 8);
                    drop(rt);

                    // Always respond with closest nodes (no local peer storage)
                    Some(krpc::build_get_peers_response_nodes(
                        tid,
                        &node.node_id,
                        &token,
                        &closest,
                    ))
                }

                "announce_peer" => {
                    // Accept announce without validation (Phase 4.3 will add token check).
                    // No local peer storage — just acknowledge.
                    Some(krpc::build_ping_response(tid, &node.node_id))
                }

                _ => None,
            }
        },
    )
}

/// Generate a token for `get_peers` / `announce_peer` validation (BEP 5).
///
/// The token is `SHA-1(secret || ip)[..4]` — simple, stateless,
/// and bound to the requesting IP address.
fn generate_token(secret: &[u8; 20], addr: &SocketAddr) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(secret);
    hasher.update(format!("{}", addr.ip()).as_bytes());
    hasher.finalize()[..4].to_vec()
}
