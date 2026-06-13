//! Discover peers via the Kademlia DHT (BEP 5).
//!
//! Bootstraps against public DHT nodes, then runs find_node and get_peers
//! queries to demonstrate the full DHT discovery flow. Requires internet.
//!
//! Run with: `cargo run -p torrent --example dht_discovery`

use torrent::dht::{DhtRpc, Node, RoutingTable, get_peers, krpc};
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let node_id = [0x42u8; 20];
    let target = [0x99u8; 20]; // arbitrary target for find_node

    // -- Step 1: Bootstrap via ping --
    println!("=== DHT Bootstrap ===\n");
    let rpc = DhtRpc::new("0.0.0.0:0".parse().unwrap())
        .await
        .expect("failed to bind DHT socket");

    let bootstrap = [
        ("router.bittorrent.com", 6881),
        ("dht.transmissionbt.com", 6881),
    ];

    let mut rt = RoutingTable::new();
    for (host, port) in &bootstrap {
        let addr: std::net::SocketAddr = match format!("{}:{}", host, port).parse() {
            Ok(a) => a,
            Err(_) => continue,
        };
        let tid: krpc::TransactionId = rand::random();
        match rpc.ping(addr, tid, &node_id).await {
            Ok(resp) => {
                let real_id = match krpc::parse_ping_response(&resp) {
                    Ok(id) => id,
                    Err(_) => continue,
                };
                rt.insert(Node { id: real_id, addr });
                println!("  ✓ {} (id: {}...)", host, hex4(&real_id[..4]));
            }
            Err(e) => {
                println!("  ✗ {} — {}", host, e);
            }
        }
    }
    println!("\nRouting table: {} nodes\n", rt.num_nodes());

    if rt.num_nodes() == 0 {
        println!("No reachable bootstrap nodes. Are you online?");
        return;
    }

    // -- Step 2: find_node — discover closer nodes --
    println!("=== find_node ===\n");
    let closest = rt.find_closest(&target, 8);
    for node in &closest {
        let tid = rand::random();
        match find_node(&rpc, node, tid, &node_id, &target).await {
            Ok(nodes) => {
                println!("  {} → {} closer nodes", node.addr, nodes.len());
                for n in &nodes[..nodes.len().min(3)] {
                    println!("    - {}... @ {}", hex4(&n.id[..4]), n.addr);
                }
            }
            Err(e) => {
                println!("  {} → failed: {}", node.addr, e);
            }
        }
    }

    // -- Step 3: get_peers — find peers for a torrent --
    println!("\n=== get_peers ===\n");
    let info_hash: [u8; 20] = rand::random();
    for node in &closest {
        let tid = rand::random();
        match get_peers(&rpc, node.addr, tid, &node_id, &info_hash).await {
            Ok(krpc::GetPeersResult::Values { peers, .. }) => {
                println!("  {} → {} peers", node.addr, peers.len());
            }
            Ok(krpc::GetPeersResult::Nodes(nodes)) => {
                println!("  {} → gave {} closer nodes", node.addr, nodes.len());
            }
            Err(e) => {
                println!("  {} → failed: {}", node.addr, e);
            }
        }
    }

    println!("\n=== Done ===");
}

/// Call find_node with proper types.
async fn find_node(
    rpc: &DhtRpc,
    node: &Node,
    tid: krpc::TransactionId,
    node_id: &[u8; 20],
    target: &[u8; 20],
) -> Result<Vec<Node>, Box<dyn std::error::Error>> {
    Ok(torrent::dht::find_node(rpc, node.addr, tid, node_id, target).await?)
}

fn hex4(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>()
}
