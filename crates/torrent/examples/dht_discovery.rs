//! Discover peers via the Kademlia DHT (BEP 5).
//!
//! This example shows the DHT API: routing table management, KRPC message
//! building, and the async RPC / query interface. Network calls are
//! commented out — bootstrap against a real DHT node to try it live.
//!
//! Run with: `cargo run -p torrent --example dht_discovery`

use torrent::dht::{DhtRpc, Node, RoutingTable, krpc};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // -- Sync: routing table --
    let mut rt = RoutingTable::new();
    println!("Empty routing table: {} nodes", rt.num_nodes());

    // Insert some bootstrap nodes
    let bootstrap = vec![
        ("router.bittorrent.com", 6881),
        ("router.utorrent.com", 6881),
    ];
    for (host, port) in &bootstrap {
        let node = Node {
            id: [0u8; 20], // real ID would come from a ping response
            addr: format!("{}:{}", host, port).parse().unwrap(),
        };
        rt.insert(node);
    }
    println!("After bootstrap: {} nodes", rt.num_nodes());

    // Find closest nodes to a target
    let target = [0x42u8; 20];
    let closest = rt.find_closest(&target, 8);
    println!("Closest {} nodes to {:02x?}:", closest.len(), &target[..4]);
    for n in &closest {
        println!("  {} @ {}", hex::encode(&n.id[..4]), n.addr);
    }

    // -- KRPC message building (sync) --
    let tid: krpc::TransactionId = rand::random();
    let node_id = [0xABu8; 20];
    let ping = krpc::build_ping(tid, &node_id);
    println!("\nPing message: {} bytes", ping.len());
    assert!(krpc::KrpcMessage::from_bytes(&ping).is_ok());

    // -- Async: DHT RPC --
    let rpc = DhtRpc::new("0.0.0.0:0".parse().unwrap())
        .await
        .expect("failed to bind DHT socket");
    println!("DHT RPC bound: OK");

    // Uncomment to send real queries to a bootstrap node:
    //
    // let info_hash = [0xABu8; 20];
    // for node in closest {
    //     let tid = rand::random();
    //     match dht::find_node(&rpc, node.addr, tid, &node_id, &target).await {
    //         Ok(nodes) => println!("find_node → {} closer nodes", nodes.len()),
    //         Err(e) => eprintln!("find_node failed: {}", e),
    //     }
    //     let tid = rand::random();
    //     match dht::get_peers(&rpc, node.addr, tid, &node_id, &info_hash).await {
    //         Ok(result) => println!("get_peers → {:?}", result),
    //         Err(e) => eprintln!("get_peers failed: {}", e),
    //     }
    // }
    let _ = rpc; // silence unused warning
}

/// Minimal hex encode for display — you can replace this with the `hex` crate.
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}
