//! Session-level DHT node — shared across all torrents.
//!
//! A single [`DhtNode`] maintains a dual-stack routing table
//! (IPv4 + IPv6 via [`DualRoutingTable`]), two UDP sockets
//! (one per address family), and one node ID (BEP 32 §3.2).
//! All torrents in the session share this node for peer discovery
//! (`get_peers`) and self-announcement (`announce_peer`).

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
use super::{DualRoutingTable, Node, find_node, generate_node_id, generate_secret, get_peers};

/// Interval between periodic bootstrap refreshes.
const BOOTSTRAP_INTERVAL: Duration = Duration::from_secs(300);

/// Number of closest nodes to query in each round of `get_peers`.
const ALPHA: usize = 8;

/// Concurrency for parallel DHT queries during lookup.
const LOOKUP_CONCURRENCY: usize = 3;

/// Cross-family want parameter — request both IPv4 and IPv6 data
/// from all DHT queries (BEP 32 §2.2: dual-stack nodes should
/// request both families during bootstrap to accelerate
/// routing table population).
const WANT_CROSS_FAMILY: Option<&[&str]> = Some(&["n4", "n6"]);

/// Dual-stack DHT node — one per session.
///
/// If IPv6 socket binding fails, the node degrades to IPv4-only
/// operation (logs a warning, `rpc_v6` set to `None`).
pub(crate) struct DhtNode {
    /// Our stable node ID (20-byte SHA-1, shared across both families).
    pub node_id: [u8; 20],
    /// Dual-stack Kademlia routing table.
    routing_table: Arc<Mutex<DualRoutingTable>>,
    /// Async UDP RPC client for IPv4 (always present).
    rpc_v4: Arc<DhtRpc>,
    /// Async UDP RPC client for IPv6 (`None` if v6 unavailable).
    rpc_v6: Option<Arc<DhtRpc>>,
    /// Well-known bootstrap addresses (resolved at construction time).
    bootstrap_nodes: Vec<SocketAddr>,
    /// Secret for token generation (BEP 5 announce_peer validation).
    secret: [u8; 20],
}

impl DhtNode {
    /// Initialize a new dual-stack DHT node.
    ///
    /// Binds both IPv4 and IPv6 UDP sockets. If IPv6 binding fails,
    /// the node degrades to IPv4-only with a warning. Resolves
    /// bootstrap hostnames from both families, creates a
    /// [`DualRoutingTable`], installs server-side query handlers,
    /// and spawns a periodic cross-family bootstrap task.
    pub async fn new(
        node_id: [u8; 20], bind_v4: SocketAddr, bind_v6: SocketAddr, bootstrap_v4: &[(&str, u16)],
        bootstrap_v6: &[(&str, u16)],
    ) -> Result<Arc<Self>, Error> {
        let rpc_v4 = DhtRpc::new(bind_v4).await?;
        let rpc_v6 = match DhtRpc::new(bind_v6).await {
            Ok(rpc) => Some(rpc),
            Err(e) => {
                tracing::warn!("IPv6 socket bind failed, continuing IPv4-only: {e}");
                None
            }
        };
        let secret = generate_secret();
        let routing_table = Arc::new(Mutex::new(DualRoutingTable::with_id(node_id)));

        let mut bootstrap_nodes = Vec::new();
        for (host, port) in bootstrap_v4.iter().chain(bootstrap_v6.iter()) {
            if let Ok(mut addrs) = tokio::net::lookup_host((*host, *port)).await {
                if let Some(addr) = addrs.next() {
                    bootstrap_nodes.push(addr);
                }
            }
        }

        let node = Arc::new(DhtNode {
            node_id,
            routing_table,
            rpc_v4,
            rpc_v6,
            bootstrap_nodes,
            secret,
        });

        // Install query handler on both sockets
        let query_handler = create_query_handler(node.clone());
        node.rpc_v4.set_query_handler(query_handler.clone());
        if let Some(ref rpc_v6) = node.rpc_v6 {
            rpc_v6.set_query_handler(query_handler);
        }

        // Kick off bootstrap in the background
        node.clone().spawn_bootstrap();

        Ok(node)
    }

    /// Bootstrap — query known bootstrap nodes to populate both routing tables.
    ///
    /// Sends `find_node` with `want=["n4","n6"]` (cross-family) to each
    /// bootstrap node. Returned nodes are inserted into the dual-stack
    /// routing table, populating both IPv4 and IPv6 buckets.
    async fn bootstrap(&self) -> () {
        for &addr in &self.bootstrap_nodes {
            let tid: krpc::TransactionId = rand::rng().random();
            let target = generate_node_id();
            let rpc = if addr.is_ipv4() {
                &self.rpc_v4
            } else if let Some(ref rpc_v6) = self.rpc_v6 {
                rpc_v6
            } else {
                continue;
            };

            if let Ok(nodes) =
                find_node(rpc, addr, tid, &self.node_id, &target, WANT_CROSS_FAMILY).await
            {
                let mut rt = self.routing_table.lock().await;
                for node in nodes {
                    rt.insert(node);
                }
            }
        }
    }

    /// Find peers for an info_hash via iterative DHT lookup (BEP 5 / BEP 32).
    ///
    /// Starts from the K closest nodes across both address families,
    /// queries them in parallel, and merges results from both DHTs.
    pub async fn get_peers(&self, info_hash: &[u8; 20]) -> Vec<SocketAddr> {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut queried: HashSet<[u8; 20]> = HashSet::new();
        let mut peers: HashSet<SocketAddr> = HashSet::new();

        let mut pending: Vec<Node> = {
            let rt = self.routing_table.lock().await;
            rt.find_closest(info_hash, ALPHA)
        };

        if pending.is_empty() {
            return vec![];
        }

        loop {
            let batch: Vec<Node> = pending
                .drain(..pending.len())
                .filter(|n| queried.insert(n.id))
                .take(LOOKUP_CONCURRENCY)
                .collect();

            if batch.is_empty() || Instant::now() >= deadline {
                break;
            }

            let mut handles = Vec::with_capacity(batch.len());
            for node in &batch {
                let rpc = if node.addr.is_ipv4() {
                    self.rpc_v4.clone()
                } else if let Some(ref rpc_v6) = self.rpc_v6 {
                    rpc_v6.clone()
                } else {
                    continue;
                };
                let node_id = self.node_id;
                let target = *info_hash;
                let addr = node.addr;
                let tid: krpc::TransactionId = rand::rng().random();

                handles.push(tokio::spawn(async move {
                    get_peers(&rpc, addr, tid, &node_id, &target, WANT_CROSS_FAMILY).await
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

            if !peers.is_empty() || Instant::now() >= deadline {
                break;
            }

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
    #[expect(dead_code)]
    pub async fn announce_peer(&self, info_hash: &[u8; 20], port: u16) -> Result<(), Error> {
        let closest = {
            let rt = self.routing_table.lock().await;
            rt.find_closest(info_hash, ALPHA)
        };

        for node in &closest {
            let tid: krpc::TransactionId = rand::rng().random();
            let data = krpc::build_announce_peer(tid, &self.node_id, info_hash, port, b"");
            let rpc: &DhtRpc = if node.addr.is_ipv4() {
                &self.rpc_v4
            } else if let Some(ref rpc_v6) = self.rpc_v6 {
                rpc_v6
            } else {
                continue;
            };
            let _ = rpc.query(node.addr, tid, &data).await;
        }

        Ok(())
    }

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
